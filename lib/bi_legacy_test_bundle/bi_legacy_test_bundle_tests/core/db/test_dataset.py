from __future__ import annotations

import uuid

import pytest
import shortuuid
import sqlalchemy as sa
import sqlalchemy.sql.sqltypes
from sqlalchemy.sql.elements import ClauseElement

from bi_constants.enums import (
    BIType, BinaryJoinOperator, ConnectionType, CreateDSFrom, DataSourceCreatedVia, DataSourceRole, JoinType,
    ManagedBy, SelectorType, WhereClauseOperation, IndexKind, OrderDirection,
)
from bi_utils.aio import await_sync, to_sync_iterable

from bi_core import exc
from bi_core.base_models import DefaultWhereClause, EntryLocation, PathEntryLocation, WorkbookEntryLocation
from bi_core.data_processing.cache.primitives import CacheTTLConfig
from bi_core.data_processing.cache.utils import SelectorCacheOptionsBuilder
from bi_core.data_processing.source_builder import SqlSourceBuilder
from bi_core.data_processing.prepared_components.primitives import PreparedMultiFromInfo
from bi_core.data_processing.prepared_components.default_manager import DefaultPreparedComponentManager
from bi_core.db import SchemaColumn, IndexInfo
from bi_core.db.native_type import ClickHouseNativeType
from bi_core.query.expression import ExpressionCtx, OrderByExpressionCtx, JoinOnExpressionCtx
from bi_core.query.bi_query import BIQuery
from bi_core.fields import BIField, ResultSchema
from bi_core.multisource import BinaryCondition, ConditionPartDirect
from bi_core_testing.database import make_table, C as TestColumn, make_sample_data
from bi_core_testing.dataset import make_dataset
from bi_core_testing.data import DataFetcher
from bi_core.us_dataset import Dataset
from bi_core.us_manager.local_cache import USEntryBuffer
from bi_core.us_manager.us_manager import USManagerBase
from bi_core.us_manager.us_manager_sync import SyncUSManager
from bi_core.utils import attrs_evolve_to_subclass
from bi_core.services_registry.top_level import ServicesRegistry
from bi_core_testing.dataset_wrappers import DatasetTestWrapper, EditableDatasetTestWrapper

from bi_connector_mssql.core.constants import CONNECTION_TYPE_MSSQL
from bi_connector_mysql.core.constants import CONNECTION_TYPE_MYSQL
from bi_connector_oracle.core.constants import CONNECTION_TYPE_ORACLE

from bi_legacy_test_bundle_tests.core.utils import (
    add_source, get_other_db, col, patch_dataset_with_two_sources, simple_groupby,
    SOURCE_TYPE_BY_CONN_TYPE,
)

# To use fixture "loop" in each test in module
#  Cause: if any other test before will use it default loop for thread will be set to None
pytestmark = pytest.mark.usefixtures("loop")


def test_save_dataset(default_sync_usm, db_table, saved_connection):
    us_manager = default_sync_usm
    dataset = make_dataset(us_manager, db_table=db_table, connection=saved_connection)
    us_manager.save(dataset)

    loaded_dataset = us_manager.get_by_id(dataset.uuid)
    ds_wrapper = DatasetTestWrapper(dataset=loaded_dataset, us_manager=us_manager)
    assert loaded_dataset.data.name == dataset.data.name
    dsrc = ds_wrapper.get_data_source_strict(source_id=dataset.get_single_data_source_id(), role=DataSourceRole.origin)
    assert dsrc.connection.uuid == saved_connection.uuid
    assert loaded_dataset.links[dataset.get_single_data_source_id()] == saved_connection.uuid

    us_manager.delete(dataset)


def test_main_data_source(
        db, db_table, saved_dataset, saved_connection, default_sync_usm,
        default_service_registry,
):
    us_manager = default_sync_usm
    sr = default_service_registry
    dataset = saved_dataset
    ds_wrapper = DatasetTestWrapper(dataset=dataset, us_manager=us_manager)

    dsrc = ds_wrapper.get_data_source_strict(
        source_id=dataset.get_single_data_source_id(), role=DataSourceRole.origin)
    assert dsrc.conn_type == db.conn_type
    assert dsrc.db_name == db.name
    assert dsrc.table_name == db_table.name
    conn_executor = sr.get_conn_executor_factory().get_sync_conn_executor(conn=dsrc.connection)
    assert dsrc.source_exists(conn_executor_factory=(lambda: conn_executor), force_refresh=True)

    schema = dsrc.get_schema_info(conn_executor_factory=lambda: conn_executor).schema
    test_schema = TestColumn.full_house()

    coltypes = {col.name: col.user_type for col in schema}
    # TODO: per-conntype type fallbacks and `almost_equal`ers.
    coltype_fallbacks = {
        BIType.boolean: [BIType.integer],
        BIType.uuid: [BIType.string],
        BIType.date: [BIType.genericdatetime],
    }

    def _make_expected_coltype_with_fallback(actual, expected):
        """ Allow for some type degradation in the expected """
        if actual == expected:
            return expected
        if actual in coltype_fallbacks.get(expected, []):
            return actual
        return expected

    expected_coltypes = {
        col.name: _make_expected_coltype_with_fallback(
            coltypes.get(col.name), col.user_type)
        for col in test_schema}
    assert coltypes == expected_coltypes

    assert dsrc.get_parameters()['table_name'] == db_table.name


def _prepare_dataset_for_caching(
        dataset: Dataset, us_manager: SyncUSManager,
        service_registry: ServicesRegistry,
):
    ds_wrapper = EditableDatasetTestWrapper(dataset=dataset, us_manager=us_manager)
    source_id = dataset.get_single_data_source_id()
    dsrc = ds_wrapper.get_data_source_strict(source_id=source_id, role=DataSourceRole.origin)
    conn_executor = service_registry.get_conn_executor_factory().get_sync_conn_executor(conn=dsrc.connection)
    ds_wrapper.add_data_source(
        source_id=source_id, role=DataSourceRole.sample, created_from=CreateDSFrom.CH_TABLE,
        parameters=dict(table_name='some_sample_table'),
        raw_schema=dsrc.get_schema_info(conn_executor_factory=lambda: conn_executor).schema,
        connection_id=dsrc.connection.uuid,
    )
    return dataset


def _build_source(dataset: Dataset, role: DataSourceRole, us_entry_buffer: USEntryBuffer) -> PreparedMultiFromInfo:
    ds_wrapper = DatasetTestWrapper(dataset=dataset, us_entry_buffer=us_entry_buffer)
    avatar_id = ds_wrapper.get_root_avatar_strict().id
    src_builder = SqlSourceBuilder()
    prepared_component_manager = DefaultPreparedComponentManager(
        role=role, dataset=dataset, us_entry_buffer=us_entry_buffer,
    )
    prepared_sources = [prepared_component_manager.get_prepared_source(
        avatar_id=avatar_id, alias=avatar_id, from_subquery=False, subquery_limit=None)]
    return src_builder.build_source(
        root_avatar_id=avatar_id,
        join_on_expressions=(),
        prepared_sources=prepared_sources,
    )


def test_data_source_cache_settings_non_mat_con_various_ds(
        default_sync_usm, default_service_registry,
        testlocal_saved_dataset,
):
    service_registry = default_service_registry
    us_manager = default_sync_usm
    us_entry_buffer = us_manager.get_entry_buffer()
    dataset = _prepare_dataset_for_caching(
        testlocal_saved_dataset,
        us_manager=default_sync_usm,
        service_registry=service_registry,
    )
    us_manager.save(dataset)
    dataset = us_manager.get_by_id(dataset.uuid, expected_type=Dataset)

    # TODO FIX: Randomize
    defined_ttl_config = CacheTTLConfig(
        ttl_sec_materialized=1430,
        ttl_sec_direct=4532,
    )
    cache_opts_builder = SelectorCacheOptionsBuilder(
        default_ttl_config=defined_ttl_config,
        us_entry_buffer=us_manager.get_entry_buffer(),
    )

    # Check origin without data_dump_id (should be short term)
    cache_opts = cache_opts_builder.get_cache_options(
        role=DataSourceRole.origin,
        query=sa.select([sa.literal(1)]), user_types=[BIType.integer],
        joint_dsrc_info=_build_source(
            dataset=dataset, role=DataSourceRole.origin, us_entry_buffer=us_entry_buffer,
        ),
        dataset=dataset,
    )
    assert (cache_opts.ttl_sec, cache_opts.refresh_ttl_on_read,) == (defined_ttl_config.ttl_sec_direct, False)


@pytest.mark.parametrize(
    ('is_bleeding_edge_user', 'expected'),
    [
        (True, True),
        (False, False),
    ],
)
def test_cache_settings_bleeding_edge(
        default_sync_usm, default_service_registry,
        testlocal_saved_dataset,
        is_bleeding_edge_user,
        expected,
):
    service_registry = default_service_registry
    us_manager = default_sync_usm
    us_entry_buffer = us_manager.get_entry_buffer()
    dataset = _prepare_dataset_for_caching(
        testlocal_saved_dataset,
        us_manager=us_manager,
        service_registry=service_registry,
    )
    us_manager.save(dataset)
    dataset = us_manager.get_by_id(dataset.uuid, expected_type=Dataset)

    # TODO FIX: Randomize
    defined_ttl_config = CacheTTLConfig(
        ttl_sec_materialized=1430,
        ttl_sec_direct=4532,
    )
    cache_opts_builder = SelectorCacheOptionsBuilder(
        default_ttl_config=defined_ttl_config,
        is_bleeding_edge_user=is_bleeding_edge_user,
        us_entry_buffer=us_manager.get_entry_buffer(),
    )

    cache_opts = cache_opts_builder.get_cache_options(
        role=DataSourceRole.origin,
        query=sa.select([sa.literal(1)]), user_types=[BIType.integer],
        joint_dsrc_info=_build_source(
            dataset=dataset, role=DataSourceRole.origin, us_entry_buffer=us_entry_buffer,
        ),
        dataset=dataset,
    )
    opt_is_here = False
    for opt in cache_opts.key.key_parts:
        if opt.part_type == 'is_bleeding_edge_user':
            assert opt.part_content == expected
            opt_is_here = True
    assert opt_is_here


def _check_get_raw_schema(
        service_registry: ServicesRegistry,
        us_manager: SyncUSManager,
        dataset: Dataset,
):
    dataset = us_manager.get_by_id(dataset.uuid, expected_type=Dataset)
    ds_wrapper = DatasetTestWrapper(dataset=dataset, us_manager=us_manager)

    expected_columns = {col.name for col in TestColumn.full_house()}

    schema = ds_wrapper.get_cached_raw_schema(role=DataSourceRole.origin)
    columns = {col.name for col in schema}
    assert columns == expected_columns

    dsrc = ds_wrapper.get_data_source_strict(
        source_id=dataset.get_single_data_source_id(), role=DataSourceRole.origin)
    conn_executor = service_registry.get_conn_executor_factory().get_sync_conn_executor(conn=dsrc.connection)
    schema = ds_wrapper.get_new_raw_schema(
        role=DataSourceRole.origin,
        conn_executor_factory=lambda: conn_executor,
    )
    columns = {col.name for col in schema}
    assert columns == expected_columns


def test_get_raw_schema(default_sync_usm, saved_dataset, default_service_registry):
    _check_get_raw_schema(default_service_registry, default_sync_usm, saved_dataset)


def test_get_raw_schema_with_schema(default_sync_usm, saved_schematized_dataset, default_service_registry):
    _check_get_raw_schema(default_service_registry, default_sync_usm, saved_schematized_dataset)


def test_get_raw_schema_for_view(default_sync_usm, saved_dataset_for_view, default_service_registry):
    _check_get_raw_schema(default_service_registry, default_sync_usm, saved_dataset_for_view)


def test_get_raw_schema_for_view_with_schema(
        default_sync_usm, saved_schematized_dataset_for_view, default_service_registry,
):
    _check_get_raw_schema(default_service_registry, default_sync_usm, saved_schematized_dataset_for_view)


def test_get_db_info(default_sync_usm, db, saved_dataset, default_service_registry):
    # check this for all dialects because it might have dialect-specific issues
    us_manager = default_sync_usm
    sr = default_service_registry
    dataset = us_manager.get_by_id(saved_dataset.uuid, expected_type=Dataset)
    ds_wrapper = DatasetTestWrapper(dataset=dataset, us_manager=us_manager)
    source_id = dataset.get_single_data_source_id()
    dsrc = ds_wrapper.get_data_source_strict(source_id=source_id, role=DataSourceRole.origin)

    conn_executor = sr.get_conn_executor_factory().get_sync_conn_executor(conn=dsrc.connection)

    db_info = dsrc.get_db_info(conn_executor_factory=(lambda: conn_executor))
    assert db_info.version is not None


@pytest.mark.parametrize("selector_type", [SelectorType.CACHED, SelectorType.CACHED_LAZY])
def test_select_data(
        default_sync_usm, db_table, saved_dataset, default_async_service_registry,
        selector_type,
):
    us_manager = default_sync_usm
    sr = default_async_service_registry
    # check this for all dialects because it might have dialect-specific issues
    dataset = us_manager.get_by_id(saved_dataset.uuid, expected_type=Dataset)
    ds_wrapper = DatasetTestWrapper(dataset=dataset, us_manager=us_manager)
    avatar_id = ds_wrapper.get_root_avatar_strict().id

    test_data = make_sample_data()
    test_data_col0 = [row['int_value'] for row in test_data]

    def make_expr(expression: ClauseElement, alias: str, user_type: BIType) -> ExpressionCtx:
        return ExpressionCtx(
            expression=expression,
            avatar_ids=[avatar_id],
            alias=alias,
            user_type=user_type,
        )

    int_value = sa.literal_column(ds_wrapper.quote('int_value', role=DataSourceRole.origin))
    role = DataSourceRole.origin

    bi_query = BIQuery(
        select_expressions=[make_expr(int_value, 'col1', BIType.integer)],
    )
    data_fetcher = DataFetcher(
        service_registry=sr, dataset=dataset, selector_type=selector_type,
        us_manager=us_manager,
    )
    data_stream = await_sync(data_fetcher.get_data_stream_async(role=role, bi_query=bi_query))

    col0_data = {row[0] for row in to_sync_iterable(data_stream.data.items)}
    col0_data_expected = {row['int_value'] for row in test_data}
    assert col0_data == col0_data_expected

    bi_query = BIQuery(
        select_expressions=[make_expr(sa.func.sum(int_value), 'col1', BIType.integer)],
        group_by_expressions=[make_expr(int_value % sa.literal(5), 'col2', BIType.integer)],
    )
    data_stream = await_sync(data_fetcher.get_data_stream_async(role=role, bi_query=bi_query))
    col0_data = {row[0] for row in to_sync_iterable(data_stream.data.items)}
    col0_data_expected = {
        sum(val)
        for val in simple_groupby(
            (val % 5, val)
            for val in test_data_col0
        ).values()}
    assert col0_data == col0_data_expected

    select_expr_ctx = make_expr(sa.func.sum(int_value), 'new_val', BIType.integer)
    bi_query = BIQuery(
        select_expressions=[select_expr_ctx],
        group_by_expressions=[make_expr(int_value % sa.literal(5), str(uuid.uuid4()), BIType.integer)],
        dimension_filters=[make_expr(int_value > 2, str(uuid.uuid4()), BIType.boolean)],
        measure_filters=[make_expr(sa.func.sum(int_value) < 12, str(uuid.uuid4()), BIType.integer)],
        order_by_expressions=[
            attrs_evolve_to_subclass(cls=OrderByExpressionCtx, inst=select_expr_ctx),
        ],
    )
    data_stream = await_sync(data_fetcher.get_data_stream_async(role=role, bi_query=bi_query))
    col0_data = [row[0] for row in to_sync_iterable(data_stream.data.items)]
    col0_data_expected = sorted([
        sum(val)
        for val in simple_groupby(
            (val % 5, val)
            for val in test_data_col0
            if val > 2
        ).values()
        if sum(val) < 12
    ])
    assert col0_data == col0_data_expected

    select_expr_ctx = make_expr(sa.func.sum(int_value), 'new_val', BIType.integer)
    bi_query = BIQuery(
        select_expressions=[select_expr_ctx],
        group_by_expressions=[make_expr(int_value % sa.literal(5), str(uuid.uuid4()), BIType.integer)],
        dimension_filters=[make_expr(int_value > 2, str(uuid.uuid4()), BIType.integer)],
        measure_filters=[make_expr(sa.func.sum(int_value) < 12, str(uuid.uuid4()), BIType.integer)],
        order_by_expressions=[
            attrs_evolve_to_subclass(cls=OrderByExpressionCtx, inst=select_expr_ctx),
        ],
        limit=2, offset=1,
    )
    data_stream = await_sync(data_fetcher.get_data_stream_async(role=role, bi_query=bi_query))
    col0_data = [row[0] for row in to_sync_iterable(data_stream.data.items)]
    col0_data_expected = sorted([
        sum(val)
        for val in simple_groupby(
            (val % 5, val)
            for val in test_data_col0
            if val > 2
        ).values()
        if sum(val) < 12
    ])[1:][:2]
    assert col0_data == col0_data_expected

    bi_query = BIQuery(
        select_expressions=[make_expr(int_value, str(uuid.uuid4()), BIType.integer)],
    )
    data_stream = await_sync(data_fetcher.get_data_stream_async(
        role=role,
        bi_query=bi_query,
        row_count_hard_limit=100,
    ))
    data = [row for row in to_sync_iterable(data_stream.data.items)]
    assert 0 < len(list(data)) <= 100

    bi_query = BIQuery(
        select_expressions=[make_expr(int_value, str(uuid.uuid4()), BIType.integer)],
        limit=3,
    )
    data_stream = await_sync(data_fetcher.get_data_stream_async(
        role=role,
        bi_query=bi_query,
        row_count_hard_limit=100,
    ))
    data = [row for row in to_sync_iterable(data_stream.data.items)]
    assert len(list(data)) == 3

    bi_query = BIQuery(
        select_expressions=[make_expr(int_value, str(uuid.uuid4()), BIType.integer)],
    )
    with pytest.raises(exc.ResultRowCountLimitExceeded):
        data_stream = await_sync(data_fetcher.get_data_stream_async(
            role=role,
            bi_query=bi_query,
            row_count_hard_limit=3,
        ))
        for row in to_sync_iterable(data_stream.data.items):
            pass

    bi_query = BIQuery(
        select_expressions=[make_expr(int_value, str(uuid.uuid4()), BIType.integer)],
        limit=20,
    )
    with pytest.raises(exc.ResultRowCountLimitExceeded):
        data_stream = await_sync(data_fetcher.get_data_stream_async(
            role=role,
            bi_query=bi_query,
            row_count_hard_limit=3,
        ))
        for row in to_sync_iterable(data_stream.data.items):
            assert row


def test_select_data_chs3(default_sync_usm, saved_chs3_dataset, default_async_service_registry):
    us_manager = default_sync_usm
    dataset = us_manager.get_by_id(saved_chs3_dataset.uuid, expected_type=Dataset)
    ds_wrapper = DatasetTestWrapper(dataset=dataset, us_manager=us_manager)
    us_manager.load_dependencies(dataset)

    test_data = make_sample_data()
    test_data_col = [row['int_value'] for row in test_data]

    role = DataSourceRole.origin
    int_value = sa.literal_column(ds_wrapper.quote('int_value', role=role))
    avatar_id = ds_wrapper.get_root_avatar_strict().id
    bi_query = BIQuery(
        select_expressions=[
            ExpressionCtx(
                expression=int_value,
                avatar_ids=[avatar_id],
                alias='col1',
                user_type=BIType.integer,
            ),
        ],
    )
    data_fetcher = DataFetcher(
        service_registry=default_async_service_registry,
        dataset=dataset, us_manager=us_manager,
    )
    data = list(data_fetcher.get_data_stream(role=role, bi_query=bi_query).data)
    assert {row[0] for row in data} == set(test_data_col)


def test_select_data_with_output_cast(default_sync_usm, db_table, saved_dataset, default_async_service_registry):
    # check this for all dialects because it might have dialect-specific issues
    us_manager = default_sync_usm
    dataset = us_manager.get_by_id(saved_dataset.uuid, expected_type=Dataset)
    ds_wrapper = DatasetTestWrapper(dataset=dataset, us_manager=us_manager)

    int_value = sa.literal_column(db_table.db.quote('int_value'))

    avatar_id = ds_wrapper.get_root_avatar_strict().id
    role = DataSourceRole.origin

    bi_query = BIQuery(
        select_expressions=[
            ExpressionCtx(
                expression=int_value,
                user_type=BIType.integer,
                avatar_ids=[avatar_id],
                alias='col1',
            ),
        ]
    )
    data_fetcher = DataFetcher(
        service_registry=default_async_service_registry,
        dataset=dataset, us_manager=us_manager,
    )
    data = list(data_fetcher.get_data_stream(role=role, bi_query=bi_query).data)
    assert {type(row[0]) for row in data} == {int}

    bi_query = BIQuery(
        select_expressions=[
            ExpressionCtx(
                expression=int_value,
                user_type=BIType.boolean,
                avatar_ids=[avatar_id],
                alias='col1',
            )
        ],
    )
    data = list(data_fetcher.get_data_stream(role=role, bi_query=bi_query).data)
    assert {type(row[0]) for row in data} == {bool}


def test_select_data_group_by_formula_field(default_sync_usm, db_table, saved_dataset, default_async_service_registry):
    us_manager = default_sync_usm
    dataset = us_manager.get_by_id(saved_dataset.uuid, expected_type=Dataset)
    ds_wrapper = DatasetTestWrapper(dataset=dataset, us_manager=us_manager)

    avatar_id = ds_wrapper.get_root_avatar_strict().id
    role = DataSourceRole.origin

    expr = ExpressionCtx(
        expression=sa.literal_column('string_value', type_=sa.String).concat(sa.bindparam('param_1', type_=sa.String)).params({'param_1': '_test'}),
        user_type=BIType.string,
        avatar_ids=[avatar_id],
        alias='col1',
    )

    bi_query = BIQuery(
        select_expressions=[expr],
        group_by_expressions=[expr],
    )
    data_fetcher = DataFetcher(
        service_registry=default_async_service_registry,
        dataset=dataset, us_manager=us_manager,
    )
    data = list(data_fetcher.get_data_stream(role=role, bi_query=bi_query).data)
    assert data
    assert all(row[0].endswith('_test') for row in data)


def test_select_data_group_by_const(default_sync_usm, db_table, saved_dataset, default_async_service_registry):
    if db_table.db.conn_type == CONNECTION_TYPE_MSSQL:
        pytest.skip('Skip test select data group by const for MSSQL because not supported')

    us_manager = default_sync_usm
    dataset = us_manager.get_by_id(saved_dataset.uuid, expected_type=Dataset)
    ds_wrapper = DatasetTestWrapper(dataset=dataset, us_manager=us_manager)

    avatar_id = ds_wrapper.get_root_avatar_strict().id
    role = DataSourceRole.origin

    expr = ExpressionCtx(
        expression=sa.literal(42),
        user_type=BIType.integer,
        avatar_ids=[avatar_id],
        alias='col1',
    )

    bi_query = BIQuery(
        select_expressions=[expr],
        group_by_expressions=[expr],
    )
    data_fetcher = DataFetcher(
        service_registry=default_async_service_registry,
        dataset=dataset, us_manager=us_manager,
    )
    data = list(data_fetcher.get_data_stream(role=role, bi_query=bi_query).data)
    assert data
    assert all(row[0] == 42 for row in data)


def test_select_data_distinct(default_sync_usm, db_table, saved_dataset, default_async_service_registry):
    # check this for all dialects because it might have dialect-specific issues
    us_manager = default_sync_usm
    dataset = us_manager.get_by_id(saved_dataset.uuid, expected_type=Dataset)
    ds_wrapper = DatasetTestWrapper(dataset=dataset, us_manager=us_manager)

    test_data = make_sample_data()
    test_data_col0 = [row['int_value'] for row in test_data]

    def make_expr(expression: ClauseElement, alias: str, user_type: BIType) -> ExpressionCtx:
        return ExpressionCtx(expression=expression, avatar_ids=[avatar_id], alias=alias, user_type=user_type)

    int_value = sa.literal_column(ds_wrapper.quote('int_value', role=DataSourceRole.origin))

    avatar_id = ds_wrapper.get_root_avatar_strict().id
    role = DataSourceRole.origin

    sel_value = int_value % 3
    bi_query = BIQuery(
        select_expressions=[make_expr(sel_value, 'sel_value', BIType.integer)],
        order_by_expressions=[
            attrs_evolve_to_subclass(
                cls=OrderByExpressionCtx,
                inst=make_expr(sel_value, 'sel_value', BIType.integer),
            )
        ],
        distinct=True,
    )
    data_fetcher = DataFetcher(
        service_registry=default_async_service_registry,
        dataset=dataset, us_manager=us_manager,
    )
    data = list(data_fetcher.get_data_stream(role=role, bi_query=bi_query).data)
    col0_data = [row[0] for row in data]
    col0_data_expected = sorted({val % 3 for val in test_data_col0})
    assert col0_data == col0_data_expected

    sel_value = sa.func.sum(int_value) % 3
    bi_query = BIQuery(
        select_expressions=[make_expr(sel_value, 'sel_value', BIType.integer)],
        group_by_expressions=[make_expr(int_value % 2, 'other_value', BIType.integer)],
        order_by_expressions=[
            attrs_evolve_to_subclass(
                cls=OrderByExpressionCtx,
                inst=make_expr(sel_value, 'sel_value', BIType.integer),
            ),
        ],
        distinct=True,
    )
    data = list(data_fetcher.get_data_stream(role=role, bi_query=bi_query).data)
    col0_data = [row[0] for row in data]
    col0_data_expected = sorted([
        sum(val) % 3
        for val in simple_groupby(
            (val % 2, val)
            for val in test_data_col0
        ).values()])
    assert col0_data == col0_data_expected


def test_get_own_materialized_tables(default_sync_usm, clickhouse_table, saved_ch_dataset):
    us_manager = default_sync_usm
    dataset = default_sync_usm.get_by_id(saved_ch_dataset.uuid, expected_type=Dataset)
    ds_wrapper = EditableDatasetTestWrapper(dataset=dataset, us_manager=us_manager)
    source_id = dataset.get_single_data_source_id()
    ds_wrapper.add_data_source(
        source_id=source_id, role=DataSourceRole.sample, created_from=CreateDSFrom.CH_TABLE,
        parameters=dict(table_name='qwerty'))
    assert list(dataset.get_own_materialized_tables()) == ['qwerty']
    assert list(dataset.get_own_materialized_tables(source_id=source_id)) == ['qwerty']
    assert list(dataset.get_own_materialized_tables(source_id='nonexistent')) == []


def test_remove_data_source_collection(
        default_sync_usm, default_service_registry,
        clickhouse_db, clickhouse_table,
        saved_ch_connection, saved_dataset_no_dsrc,
):
    service_registry = default_service_registry
    us_manager = default_sync_usm
    dataset = us_manager.get_by_id(saved_dataset_no_dsrc.uuid, expected_type=Dataset)
    ds_wrapper = EditableDatasetTestWrapper(dataset=dataset, us_manager=us_manager)
    add_source(
        dataset, saved_ch_connection,
        db=clickhouse_db, table_name=clickhouse_table.name,
        us_manager=us_manager,
        service_registry=service_registry,
    )
    ds_wrapper.remove_data_source_collection(source_id=dataset.get_single_data_source_id())
    new_source_id = str(uuid.uuid4())
    ds_wrapper.add_data_source(
        source_id=new_source_id, role=DataSourceRole.origin, created_from=CreateDSFrom.CH_TABLE,
        parameters=dict(db_name=clickhouse_table.db.name, table_name=clickhouse_table.name),
        connection_id=saved_ch_connection.uuid,
    )
    assert ds_wrapper.get_data_source_opt(source_id=new_source_id, role=DataSourceRole.origin) is not None


def test_remove_data_source(default_sync_usm, clickhouse_table, saved_ch_dataset):
    us_manager = default_sync_usm
    dataset = us_manager.get_by_id(saved_ch_dataset.uuid, expected_type=Dataset)
    ds_wrapper = EditableDatasetTestWrapper(dataset=dataset, us_manager=us_manager)
    source_id = dataset.get_single_data_source_id()
    ds_wrapper.add_data_source(
        source_id=source_id, role=DataSourceRole.sample, created_from=CreateDSFrom.CH_TABLE,
        parameters=dict(table_name='qwerty'))
    dsrc_coll = ds_wrapper.get_data_source_coll_strict(source_id=source_id)
    assert dsrc_coll.get_opt(role=DataSourceRole.sample) is not None

    ds_wrapper.remove_data_source(source_id=source_id, role=DataSourceRole.sample)
    dsrc_coll = ds_wrapper.get_data_source_coll_strict(source_id=source_id)
    assert DataSourceRole.sample not in dsrc_coll


def test_select_data_single_source_avatar(
        default_sync_usm,
        clickhouse_table,
        saved_ch_dataset,
        default_async_service_registry,
):
    us_manager = default_sync_usm
    dataset = us_manager.get_by_id(saved_ch_dataset.uuid, expected_type=Dataset)
    ds_wrapper = DatasetTestWrapper(dataset=dataset, us_manager=us_manager)

    test_data = make_sample_data()
    test_data_col0 = [row['int_value'] for row in test_data]

    avatar_id = ds_wrapper.get_root_avatar_strict().id
    role = DataSourceRole.origin

    int_value = sa.literal_column(ds_wrapper.quote('int_value', role=DataSourceRole.origin))
    bi_query = BIQuery(
        select_expressions=[
            ExpressionCtx(
                expression=int_value,
                avatar_ids=[avatar_id],
                alias='col1',
                user_type=BIType.integer,
            ),
        ],
    )
    data_fetcher = DataFetcher(
        service_registry=default_async_service_registry,
        dataset=dataset, us_manager=us_manager,
    )
    data = list(data_fetcher.get_data_stream(role=role, bi_query=bi_query).data)
    assert {row[0] for row in data} == set(test_data_col0)


def test_order_by_w_nulls(
        default_sync_usm,
        saved_dataset,
        default_async_service_registry,
):
    us_manager = default_sync_usm
    dataset = us_manager.get_by_id(saved_dataset.uuid, expected_type=Dataset)
    ds_wrapper = DatasetTestWrapper(dataset=dataset, us_manager=us_manager)

    test_data = make_sample_data()
    half = len(test_data) // 2

    avatar_id = ds_wrapper.get_root_avatar_strict().id
    role = DataSourceRole.origin

    int_value_col = sa.literal_column(ds_wrapper.quote('int_value', role=DataSourceRole.origin))
    nullable_int_value = sa.case(
        whens=[((int_value_col % 2) == 0, int_value_col)],
        else_=sa.null(),
    )

    def get_data(direction: OrderDirection):
        bi_query = BIQuery(
            select_expressions=[
                ExpressionCtx(
                    expression=nullable_int_value,
                    avatar_ids=[avatar_id],
                    alias='col1',
                    user_type=BIType.integer,
                ),
            ],
            order_by_expressions=[
                OrderByExpressionCtx(
                    direction=direction,
                    expression=nullable_int_value,
                    avatar_ids=[avatar_id],
                    alias='col1',
                    user_type=BIType.integer,
                ),
            ]
        )
        data_fetcher = DataFetcher(
            service_registry=default_async_service_registry,
            dataset=dataset, us_manager=us_manager,
        )
        data = list(data_fetcher.get_data_stream(role=role, bi_query=bi_query).data)
        return data

    data = get_data(direction=OrderDirection.asc)
    assert [row[0] for row in data[:half]] == [None] * half

    data = get_data(direction=OrderDirection.desc)
    assert [row[0] for row in data[half:]] == [None] * half


class SomeSelectTestBase:
    @pytest.fixture
    def dataset_id(self):
        raise NotImplementedError

    @pytest.fixture
    def dataset(self, dataset_id, default_sync_usm):
        us_manager = default_sync_usm
        dataset = us_manager.get_by_id(dataset_id, expected_type=Dataset)
        us_manager.load_dependencies(dataset)
        return dataset

    def _get_data_stream(
            self, dataset, service_registry, bi_query,
            us_manager: USManagerBase, role=DataSourceRole.origin,
    ):
        """ Simple single-source data stream helper """
        data_fetcher = DataFetcher(
            service_registry=service_registry,
            dataset=dataset, us_manager=us_manager,
        )
        data_stream = data_fetcher.get_data_stream(role=role, bi_query=bi_query)
        return data_stream

    def test_main(self):
        pass


class TestPGEnforceCollate(SomeSelectTestBase):
    @pytest.fixture
    def dataset_id(self, saved_pg_enforce_collate_dataset):
        return saved_pg_enforce_collate_dataset.uuid

    def test_main(self, dataset, default_async_service_registry, default_sync_usm):
        us_manager = default_sync_usm
        ds_wrapper = DatasetTestWrapper(dataset=dataset, us_manager=us_manager)
        avatar_id = ds_wrapper.get_root_avatar_strict().id
        # To make it work, need `create collation en_US from "en_US";` on the database.
        # But in this case, testing how postgresql works is not the point,
        with pytest.raises(exc.DatabaseQueryError) as exc_info:
            self._get_data_stream(
                dataset=dataset,
                service_registry=default_async_service_registry,
                us_manager=us_manager,
                bi_query=BIQuery(
                    select_expressions=[
                        ExpressionCtx(
                            expression=sa.func.lower(sa.literal_column('n_string_value')),
                            avatar_ids=[avatar_id],
                            alias='str_l',
                            user_type=BIType.string,
                        ),
                        ExpressionCtx(
                            expression=sa.func.upper(sa.literal_column('n_string_value')),
                            avatar_ids=[avatar_id],
                            alias='str_u',
                            user_type=BIType.string,
                        ),
                    ],
                    limit=9,
                ),
            )
        err = exc_info.value
        assert ' COLLATE ' in err.query
        msg = err.debug_info['db_message']
        assert msg.startswith('collation "en_US" for encoding "UTF8" does not exist')


class TestSelectDataCHSubselect(SomeSelectTestBase):
    @pytest.fixture
    def dataset_id(self, saved_ch_subselect_dataset):
        return saved_ch_subselect_dataset.uuid

    def test_main(self, dataset, default_async_service_registry, default_sync_usm):
        us_manager = default_sync_usm
        ds_wrapper = DatasetTestWrapper(dataset=dataset, us_manager=us_manager)
        avatar_id = ds_wrapper.get_root_avatar_strict().id
        stream = self._get_data_stream(
            dataset=dataset,
            service_registry=default_async_service_registry,
            us_manager=us_manager,
            bi_query=BIQuery(
                select_expressions=[
                    ExpressionCtx(
                        expression=sa.literal_column('number'),
                        avatar_ids=[avatar_id],
                        alias='col1',
                        user_type=BIType.integer,
                    ),
                    ExpressionCtx(
                        expression=sa.literal_column('str'),
                        avatar_ids=[avatar_id],
                        alias='col2',
                        user_type=BIType.string,
                    ),
                ],
                limit=9,
            ),
        )
        data = list(stream.data)
        # `7` is the entire amount of rows in the subselect (`arrayJoin(range(7))`).
        assert len(list(data)) == 7


def test_select_data_multiple_source_avatars(
        default_sync_usm,
        db,
        saved_connection,
        multiple_db_tables,
        saved_dataset_no_dsrc,
        default_service_registry,
        default_async_service_registry,
):
    mt = multiple_db_tables
    connection = saved_connection
    sync_service_registry = default_service_registry
    us_manager = default_sync_usm
    dataset = us_manager.get_by_id(saved_dataset_no_dsrc.uuid, expected_type=Dataset)
    ds_wrapper = EditableDatasetTestWrapper(dataset=dataset, us_manager=us_manager)

    users_dsrc_id = add_source(
        dataset=dataset, connection=connection,
        db=db, table_name=mt.users.name,
        us_manager=us_manager,
        service_registry=sync_service_registry,
    )
    posts_dsrc_id = add_source(
        dataset=dataset, connection=connection,
        db=db, table_name=mt.posts.name,
        us_manager=us_manager,
        service_registry=sync_service_registry,
    )
    comments_dsrc_id = add_source(
        dataset=dataset, connection=connection,
        db=db, table_name=mt.comments.name,
        us_manager=us_manager,
        service_registry=sync_service_registry,
    )

    from_user_avatar_id = str(uuid.uuid4())
    ds_wrapper.add_avatar(avatar_id=from_user_avatar_id, source_id=users_dsrc_id, title='From User')
    to_user_avatar_id = str(uuid.uuid4())
    ds_wrapper.add_avatar(avatar_id=to_user_avatar_id, source_id=users_dsrc_id, title='To User')
    author_avatar_id = str(uuid.uuid4())
    ds_wrapper.add_avatar(avatar_id=author_avatar_id, source_id=users_dsrc_id, title='Author')
    post_avatar_id = str(uuid.uuid4())
    ds_wrapper.add_avatar(avatar_id=post_avatar_id, source_id=posts_dsrc_id, title='Post')
    comment_avatar_id = str(uuid.uuid4())
    ds_wrapper.add_avatar(avatar_id=comment_avatar_id, source_id=comments_dsrc_id, title='Comment')

    # Table structure:
    # Comment (comments) --- From user (users)
    #                    |-- To user (users)
    #                    *-- Post (posts)      --- Author (users)

    relation_1_id = str(uuid.uuid4())
    ds_wrapper.add_avatar_relation(
        relation_id=relation_1_id,
        left_avatar_id=comment_avatar_id, right_avatar_id=from_user_avatar_id,
        conditions=[
            BinaryCondition(
                operator=BinaryJoinOperator.eq,
                left_part=ConditionPartDirect(source='from_id'),
                right_part=ConditionPartDirect(source='id'),
            )
        ]
    )
    relation_2_id = str(uuid.uuid4())
    ds_wrapper.add_avatar_relation(
        relation_id=relation_2_id,
        left_avatar_id=comment_avatar_id, right_avatar_id=to_user_avatar_id,
        conditions=[
            BinaryCondition(
                operator=BinaryJoinOperator.eq,
                left_part=ConditionPartDirect(source='to_id'),
                right_part=ConditionPartDirect(source='id'),
            )
        ]
    )
    relation_3_id = str(uuid.uuid4())
    ds_wrapper.add_avatar_relation(
        relation_id=relation_3_id,
        left_avatar_id=comment_avatar_id, right_avatar_id=post_avatar_id,
        conditions=[
            BinaryCondition(
                operator=BinaryJoinOperator.eq,
                left_part=ConditionPartDirect(source='post_id'),
                right_part=ConditionPartDirect(source='id'),
            )
        ]
    )
    relation_4_id = str(uuid.uuid4())
    ds_wrapper.add_avatar_relation(
        relation_id=relation_4_id,
        left_avatar_id=post_avatar_id, right_avatar_id=author_avatar_id,
        conditions=[
            BinaryCondition(
                operator=BinaryJoinOperator.eq,
                left_part=ConditionPartDirect(source='author_id'),
                right_part=ConditionPartDirect(source='id'),
            )
        ]
    )

    role = DataSourceRole.origin
    join_on_expressions = [
        JoinOnExpressionCtx(
            expression=col(ds_wrapper, comment_avatar_id, 'from_id') == col(ds_wrapper, from_user_avatar_id, 'id'),
            avatar_ids=[comment_avatar_id, from_user_avatar_id],
            user_type=BIType.boolean,
            join_type=JoinType.inner,
            left_id=comment_avatar_id,
            right_id=from_user_avatar_id,
        ),
        JoinOnExpressionCtx(
            expression=col(ds_wrapper, comment_avatar_id, 'to_id') == col(ds_wrapper, to_user_avatar_id, 'id'),
            avatar_ids=[comment_avatar_id, to_user_avatar_id],
            user_type=BIType.boolean,
            join_type=JoinType.inner,
            left_id=comment_avatar_id,
            right_id=to_user_avatar_id,
        ),
        JoinOnExpressionCtx(
            expression=col(ds_wrapper, comment_avatar_id, 'post_id') == col(ds_wrapper, post_avatar_id, 'id'),
            avatar_ids=[comment_avatar_id, post_avatar_id],
            user_type=BIType.boolean,
            join_type=JoinType.inner,
            left_id=comment_avatar_id,
            right_id=post_avatar_id,
        ),
        JoinOnExpressionCtx(
            expression=col(ds_wrapper, post_avatar_id, 'author_id') == col(ds_wrapper, author_avatar_id, 'id'),
            avatar_ids=[post_avatar_id, author_avatar_id],
            user_type=BIType.boolean,
            join_type=JoinType.inner,
            left_id=post_avatar_id,
            right_id=author_avatar_id,
        ),
    ]
    root_avatar_id = comment_avatar_id
    required_avatar_ids = [
        comment_avatar_id,
        from_user_avatar_id,
        to_user_avatar_id,
        post_avatar_id,
        author_avatar_id,
    ]

    bi_query = BIQuery(
        select_expressions=[
            ExpressionCtx(
                expression=col(ds_wrapper, comment_avatar_id, 'id'), avatar_ids=[comment_avatar_id],
                alias='col1', user_type=BIType.integer,
            ),
            ExpressionCtx(
                expression=col(ds_wrapper, post_avatar_id, 'text'), avatar_ids=[post_avatar_id],
                alias='col2', user_type=BIType.string,
            ),
            ExpressionCtx(
                expression=sa.literal('by', type_=sa.String), avatar_ids=[],
                alias='col3', user_type=BIType.string,
            ),
            ExpressionCtx(
                expression=col(ds_wrapper, author_avatar_id, 'name'), avatar_ids=[author_avatar_id],
                alias='col4', user_type=BIType.string,
            ),
            ExpressionCtx(
                expression=sa.literal('> comment >', type_=sa.String), avatar_ids=[],
                alias='col5', user_type=BIType.string,
            ),
            ExpressionCtx(
                expression=col(ds_wrapper, comment_avatar_id, 'text'), avatar_ids=[comment_avatar_id],
                alias='col6', user_type=BIType.string,
            ),
            ExpressionCtx(
                expression=sa.literal('from', type_=sa.String), avatar_ids=[],
                alias='col7', user_type=BIType.string,
            ),
            ExpressionCtx(
                expression=col(ds_wrapper, from_user_avatar_id, 'name'), avatar_ids=[from_user_avatar_id],
                alias='col8', user_type=BIType.string,
            ),
            ExpressionCtx(
                expression=sa.literal('to', type_=sa.String), avatar_ids=[],
                alias='col9', user_type=BIType.string,
            ),
            ExpressionCtx(
                expression=col(ds_wrapper, to_user_avatar_id, 'name'), avatar_ids=[to_user_avatar_id],
                alias='col10', user_type=BIType.string,
            ),
        ],
        order_by_expressions=[
            OrderByExpressionCtx(
                expression=col(ds_wrapper, comment_avatar_id, 'id'), avatar_ids=[comment_avatar_id],
                alias='col1', user_type=BIType.integer,
            ),
            OrderByExpressionCtx(
                expression=sa.literal('by', type_=sa.String), avatar_ids=[],
                alias='col3', user_type=BIType.string,
            ),
            OrderByExpressionCtx(
                expression=sa.literal('> comment >', type_=sa.String), avatar_ids=[],
                alias='col5', user_type=BIType.string,
            ),
            OrderByExpressionCtx(
                expression=sa.literal('from', type_=sa.String), avatar_ids=[],
                alias='col7', user_type=BIType.string,
            ),
            OrderByExpressionCtx(
                expression=sa.literal('to', type_=sa.String), avatar_ids=[],
                alias='col9', user_type=BIType.string,
            ),
        ],
    )
    data_fetcher = DataFetcher(
        service_registry=default_async_service_registry,
        dataset=dataset, us_manager=us_manager,
    )
    data = list(data_fetcher.get_data_stream(
        role=role,
        bi_query=bi_query,
        root_avatar_id=root_avatar_id,
        required_avatar_ids=required_avatar_ids,
        join_on_expressions=join_on_expressions,
    ).data)

    lines = ['{}. {}'.format(row[0], ' '.join(row[1:])) for row in data]

    assert lines == [
        '1. Let it be by Gaius Marius > comment > first from Clark Kent to Gaius Marius',
        '2. Let it be by Gaius Marius > comment > second from Gaius Marius to Clark Kent',
        '3. Let it be by Gaius Marius > comment > third from Charlie Chaplin to Gaius Marius',
        '4. Help by Rosalind Franklin > comment > first from Charlie Chaplin to Rosalind Franklin',
        '5. Help by Rosalind Franklin > comment > second from Rosalind Franklin to Charlie Chaplin',
        '6. Help by Rosalind Franklin > comment > third from Clark Kent to Rosalind Franklin',
    ]

    # Test a query with GROUP BY
    # (same expressions in SELECT as in GROUP BY)
    col_expr_contexts = [
        ExpressionCtx(
            expression=col(ds_wrapper, author_avatar_id, 'name'),
            alias='col1',
            avatar_ids=[author_avatar_id],
            user_type=BIType.string,
        ),
        ExpressionCtx(
            expression=col(ds_wrapper, from_user_avatar_id, 'name'),
            alias='col2',
            avatar_ids=[from_user_avatar_id],
            user_type=BIType.string,
        ),
        ExpressionCtx(
            expression=col(ds_wrapper, to_user_avatar_id, 'name'),
            alias='col3',
            avatar_ids=[to_user_avatar_id],
            user_type=BIType.string,
        ),
    ]
    bi_query = BIQuery(
        select_expressions=col_expr_contexts,
        group_by_expressions=col_expr_contexts
    )
    data = list(data_fetcher.get_data_stream(
        role=role,
        bi_query=bi_query,
        root_avatar_id=root_avatar_id,
        required_avatar_ids=required_avatar_ids,
        join_on_expressions=join_on_expressions,
    ).data)
    assert len(data) == 6

    # Test BI-1050
    # (GROUP BY contains expressions not present in SELECT)
    bi_query = BIQuery(
        select_expressions=[
            ExpressionCtx(
                expression=col(ds_wrapper, from_user_avatar_id, 'name'), alias='col2',
                avatar_ids=[from_user_avatar_id], user_type=BIType.string,
            ),
        ],
        group_by_expressions=[
            ExpressionCtx(
                expression=col(ds_wrapper, post_avatar_id, 'text'), alias='col0',
                avatar_ids=[post_avatar_id], user_type=BIType.string,
            ),
            ExpressionCtx(
                expression=col(ds_wrapper, author_avatar_id, 'name'), alias='col1',
                avatar_ids=[author_avatar_id], user_type=BIType.string,
            ),
            ExpressionCtx(
                expression=col(ds_wrapper, from_user_avatar_id, 'name'), alias='col2',
                avatar_ids=[from_user_avatar_id], user_type=BIType.string,
            ),
            ExpressionCtx(
                expression=col(ds_wrapper, to_user_avatar_id, 'name'), alias='col3',
                avatar_ids=[to_user_avatar_id], user_type=BIType.string,
            ),
        ]
    )
    data = list(data_fetcher.get_data_stream(
        role=role,
        bi_query=bi_query,
        root_avatar_id=root_avatar_id,
        required_avatar_ids=required_avatar_ids,
        join_on_expressions=join_on_expressions,
    ).data)
    assert len(data) == 6


def test_manage_source_avatars(
        default_sync_usm, clickhouse_db, clickhouse_table, saved_ch_connection,
        saved_dataset_no_dsrc, app_request_context, default_service_registry
):
    service_registry = default_service_registry
    us_manager = default_sync_usm
    dataset = us_manager.get_by_id(saved_dataset_no_dsrc.uuid, expected_type=Dataset)
    ds_wrapper = EditableDatasetTestWrapper(dataset=dataset, us_manager=us_manager)

    source_id = add_source(
        dataset=dataset, connection=saved_ch_connection,
        db=clickhouse_db, table_name=clickhouse_table.name,
        us_manager=us_manager, service_registry=service_registry,
    )

    avatar_1_id = str(uuid.uuid4())
    ds_wrapper.add_avatar(avatar_id=avatar_1_id, source_id=source_id, title='Avatar 1')
    avatar_2_id = str(uuid.uuid4())
    ds_wrapper.add_avatar(avatar_id=avatar_2_id, source_id=source_id, title='Avatar 2')

    avatars = ds_wrapper.get_avatar_list()
    assert len(avatars) == 2
    assert avatars[0].is_root
    assert not avatars[1].is_root
    assert len(ds_wrapper.get_avatar_list(source_id=source_id)) == 2
    assert len(ds_wrapper.get_avatar_list(source_id=str(uuid.uuid4()))) == 0

    ds_wrapper.update_avatar(avatar_id=avatar_1_id, title='Renamed Avatar 1')
    assert ds_wrapper.get_avatar_strict(avatar_id=avatar_1_id).title == 'Renamed Avatar 1'

    ds_wrapper.remove_avatar(avatar_id=avatar_1_id)
    assert len(ds_wrapper.get_avatar_list()) == 1
    # avatar_2 is now root after avatar_1 was deleted
    assert ds_wrapper.get_avatar_strict(avatar_id=avatar_2_id).is_root


def test_manage_avatar_relations(
        default_sync_usm, clickhouse_db, clickhouse_table, saved_ch_connection,
        saved_dataset_no_dsrc, app_request_context, default_service_registry,
):
    service_registry = default_service_registry
    us_manager = default_sync_usm
    dataset = us_manager.get_by_id(saved_dataset_no_dsrc.uuid, expected_type=Dataset)
    ds_wrapper = EditableDatasetTestWrapper(dataset=dataset, us_manager=us_manager)

    source_id = add_source(
        dataset=dataset, connection=saved_ch_connection,
        db=clickhouse_db, table_name=clickhouse_table.name,
        us_manager=us_manager, service_registry=service_registry,
    )

    # Add avatar_2 first so that we can check that the root avatar is reset
    # to avatar_1 when relations are defined
    avatar_2_id = str(uuid.uuid4())
    ds_wrapper.add_avatar(avatar_id=avatar_2_id, source_id=source_id, title='Avatar 2')
    avatar_1_id = str(uuid.uuid4())
    ds_wrapper.add_avatar(avatar_id=avatar_1_id, source_id=source_id, title='Avatar 1')
    avatar_3_id = str(uuid.uuid4())
    ds_wrapper.add_avatar(avatar_id=avatar_3_id, source_id=source_id, title='Avatar 3')
    avatar_4_id = str(uuid.uuid4())
    ds_wrapper.add_avatar(avatar_id=avatar_4_id, source_id=source_id, title='Avatar 4')

    # avatar_2 should still be root
    assert not ds_wrapper.get_avatar_strict(avatar_id=avatar_1_id).is_root
    assert ds_wrapper.get_avatar_strict(avatar_id=avatar_2_id).is_root

    rel_1_id = str(uuid.uuid4())
    ds_wrapper.add_avatar_relation(
        relation_id=rel_1_id,
        left_avatar_id=avatar_1_id, right_avatar_id=avatar_2_id,
        conditions=[
            BinaryCondition(
                operator=BinaryJoinOperator.eq,
                left_part=ConditionPartDirect(source='int_value'),
                right_part=ConditionPartDirect(source='int_value'),
            )
        ]
    )
    # root should have been reset to avatar_1
    assert ds_wrapper.get_avatar_strict(avatar_id=avatar_1_id).is_root
    assert not ds_wrapper.get_avatar_strict(avatar_id=avatar_2_id).is_root

    rel_2_id = str(uuid.uuid4())
    ds_wrapper.add_avatar_relation(
        relation_id=rel_2_id,
        left_avatar_id=avatar_1_id, right_avatar_id=avatar_3_id,
        conditions=[
            BinaryCondition(
                operator=BinaryJoinOperator.eq,
                left_part=ConditionPartDirect(source='int_value'),
                right_part=ConditionPartDirect(source='int_value'),
            )
        ]
    )
    rel_3_id = str(uuid.uuid4())
    ds_wrapper.add_avatar_relation(
        relation_id=rel_3_id,
        left_avatar_id=avatar_2_id, right_avatar_id=avatar_4_id,
        conditions=[
            BinaryCondition(
                operator=BinaryJoinOperator.eq,
                left_part=ConditionPartDirect(source='author_id'),
                right_part=ConditionPartDirect(source='int_value'),
            )
        ]
    )

    assert len(ds_wrapper.get_avatar_relation_list()) == 3
    assert len(ds_wrapper.get_avatar_relation_list(left_avatar_id=avatar_1_id)) == 2
    assert len(ds_wrapper.get_avatar_relation_list(left_avatar_id=str(uuid.uuid4()))) == 0
    assert len(ds_wrapper.get_avatar_relation_list(right_avatar_id=avatar_2_id)) == 1

    ds_wrapper.update_avatar_relation(
        relation_id=rel_1_id,
        conditions=[
            BinaryCondition(
                operator=BinaryJoinOperator.eq,
                left_part=ConditionPartDirect(source='int_value'),
                right_part=ConditionPartDirect(source='int_value'),
            ),
            BinaryCondition(
                operator=BinaryJoinOperator.eq,
                left_part=ConditionPartDirect(source='datetime_value'),
                right_part=ConditionPartDirect(source='datetime_value'),
            )
        ]
    )
    assert len(ds_wrapper.get_avatar_relation_strict(relation_id=rel_1_id).conditions) == 2

    ds_wrapper.remove_avatar_relation(relation_id=rel_3_id)
    assert len(ds_wrapper.get_avatar_relation_list()) == 2


def test_find_data_source_configuration(
        default_sync_usm, clickhouse_db, two_clickhouse_tables,
        saved_ch_connection, saved_dataset_no_dsrc, app_request_context,
        default_service_registry
):
    service_registry = default_service_registry
    us_manager = default_sync_usm
    db = clickhouse_db
    db_table_1, db_table_2 = two_clickhouse_tables
    connection = saved_ch_connection
    dataset = us_manager.get_by_id(saved_dataset_no_dsrc.uuid, expected_type=Dataset)
    source_1_id = add_source(
        dataset=dataset, connection=connection,
        db=db, table_name=db_table_1.name,
        us_manager=us_manager, service_registry=service_registry,
    )
    source_2_id = add_source(
        dataset=dataset, connection=connection,
        db=db, table_name=db_table_2.name,
        us_manager=us_manager, service_registry=service_registry,
    )

    assert dataset.find_data_source_configuration(
        connection_id=connection.uuid, created_from=CreateDSFrom.CH_TABLE,
        parameters=dict(table_name=db_table_1.name),
    ) == source_1_id
    assert dataset.find_data_source_configuration(
        connection_id=connection.uuid, created_from=CreateDSFrom.CH_TABLE,
        parameters=dict(table_name=db_table_2.name),
    ) == source_2_id
    assert dataset.find_data_source_configuration(
        connection_id=connection.uuid, created_from=CreateDSFrom.CH_TABLE,
        parameters=dict(table_name='fake_table'),
    ) is None
    assert dataset.find_data_source_configuration(
        connection_id=connection.uuid, created_from=CreateDSFrom.PG_TABLE,
        parameters=dict(table_name='second_table'),
    ) is None


def test_create_result_schema_field(default_sync_usm, saved_ch_dataset, app_request_context):
    us_manager = default_sync_usm
    dataset = us_manager.get_by_id(saved_ch_dataset.uuid, expected_type=Dataset)
    column = SchemaColumn(
        name='int_value',
        title='int_value',
        user_type=BIType.integer,
        nullable=True,
        native_type=ClickHouseNativeType.normalize_name_and_create(
            conn_type=ConnectionType.clickhouse, name='int64'),
        source_id=dataset.get_single_data_source_id(),
    )
    field_data = dataset.create_result_schema_field(column=column)
    assert field_data['title'] == 'int_value'
    assert field_data['cast'] == BIType.integer.name
    assert field_data['data_type'] == BIType.integer.name

    ds_wrapper = EditableDatasetTestWrapper(dataset=dataset, us_manager=us_manager)
    ds_wrapper.set_result_schema(ResultSchema([BIField.make(**field_data)]))

    field_data = dataset.create_result_schema_field(column=column)
    assert field_data['title'] == 'int_value (1)'


def _check_schematized_table_dataset(sync_usm, connection, table, service_registry, async_service_registry):
    dataset = make_dataset(sync_usm)
    us_manager = sync_usm
    ds_wrapper = EditableDatasetTestWrapper(dataset=dataset, us_manager=us_manager)
    add_source(
        dataset=dataset, connection=connection, db=table.db,
        schema_name=table.schema, table_name=table.name,
        us_manager=us_manager, service_registry=service_registry,
    )
    ds_wrapper.add_avatar(
        avatar_id=str(uuid.uuid4()), source_id=dataset.get_single_data_source_id(), title='Main Avatar')
    sync_usm.load_dependencies(dataset)

    test_data = make_sample_data()
    test_data_col0 = [row['int_value'] for row in test_data]

    int_value = sa.literal_column(ds_wrapper.quote('int_value', role=DataSourceRole.origin))
    avatar_id = ds_wrapper.get_root_avatar_strict().id
    role = DataSourceRole.origin
    bi_query = BIQuery(
        select_expressions=[
            ExpressionCtx(
                expression=int_value,
                avatar_ids=[avatar_id],
                user_type=BIType.integer,
                alias='col1',
            )
        ],
    )
    data_fetcher = DataFetcher(
        service_registry=async_service_registry,
        dataset=dataset, us_manager=us_manager,
    )
    data = list(data_fetcher.get_data_stream(role=role, bi_query=bi_query).data)
    assert {row[0] for row in data} == set(test_data_col0)


def test_schematized_table_dataset(
        default_sync_usm,
        saved_schematized_connection,
        schematized_db_table,
        default_service_registry,
        default_async_service_registry,
):
    _check_schematized_table_dataset(
        default_sync_usm, connection=saved_schematized_connection,
        table=schematized_db_table,
        service_registry=default_service_registry,
        async_service_registry=default_async_service_registry,
    )


def test_cross_db_multisource_dataset(
        clickhouse_db,
        saved_dataset_no_dsrc,
        saved_ch_connection,
        default_service_registry,
        default_async_service_registry,
        default_sync_usm,
):
    connection = saved_ch_connection
    dataset = saved_dataset_no_dsrc
    synbc_service_registry = default_service_registry
    us_manager = default_sync_usm
    ds_wrapper = EditableDatasetTestWrapper(dataset=dataset, us_manager=us_manager)

    test_data = make_sample_data()
    test_data_col0 = [row['int_value'] for row in test_data]

    this_db = clickhouse_db
    this_table = make_table(this_db)
    other_db = get_other_db(conn_type=this_db.conn_type)
    other_table = make_table(other_db)

    this_dsrc_id = add_source(
        dataset=dataset, connection=connection,
        db=this_db, table_name=this_table.name,
        us_manager=us_manager, service_registry=synbc_service_registry,
    )
    other_dsrc_id = add_source(
        dataset=dataset, connection=connection,
        db=other_db, table_name=other_table.name,
        us_manager=us_manager, service_registry=synbc_service_registry,
    )

    this_avatar_id = str(uuid.uuid4())
    ds_wrapper.add_avatar(avatar_id=this_avatar_id, source_id=this_dsrc_id, title='This')
    other_avatar_id = str(uuid.uuid4())
    ds_wrapper.add_avatar(avatar_id=other_avatar_id, source_id=other_dsrc_id, title='Other')

    relation_id = str(uuid.uuid4())
    ds_wrapper.add_avatar_relation(
        relation_id=relation_id,
        left_avatar_id=this_avatar_id, right_avatar_id=other_avatar_id,
        conditions=[
            BinaryCondition(
                operator=BinaryJoinOperator.eq,
                left_part=ConditionPartDirect(source='int_value'),
                right_part=ConditionPartDirect(source='int_value'),
            )
        ]
    )

    role = DataSourceRole.origin
    join_on_expressions = [
        JoinOnExpressionCtx(
            expression=col(ds_wrapper, this_avatar_id, 'int_value') == col(ds_wrapper, other_avatar_id, 'int_value'),
            user_type=BIType.boolean,
            avatar_ids=[this_avatar_id, other_avatar_id],
            left_id=this_avatar_id,
            right_id=other_avatar_id,
            join_type=JoinType.inner
        ),
    ]
    root_avatar_id = this_avatar_id
    required_avatar_ids = [this_avatar_id, other_avatar_id]

    bi_query = BIQuery(
        select_expressions=[
            ExpressionCtx(
                expression=col(ds_wrapper, this_avatar_id, 'int_value'),
                avatar_ids=[this_avatar_id],
                alias='col1',
                user_type=BIType.integer,
            ),
            ExpressionCtx(
                expression=col(ds_wrapper, other_avatar_id, 'int_value'),
                avatar_ids=[other_avatar_id],
                alias='col2',
                user_type=BIType.integer,
            ),
        ],
        order_by_expressions=[
            OrderByExpressionCtx(
                expression=col(ds_wrapper, this_avatar_id, 'int_value'),
                avatar_ids=[this_avatar_id],
                alias='col1',
                user_type=BIType.integer,
            ),
        ],
    )
    data_fetcher = DataFetcher(
        service_registry=default_async_service_registry,
        dataset=dataset, us_manager=us_manager,
    )
    data = list(data_fetcher.get_data_stream(
        role=role,
        bi_query=bi_query,
        root_avatar_id=root_avatar_id,
        required_avatar_ids=required_avatar_ids,
        join_on_expressions=join_on_expressions,
    ).data)

    expected_data = [
        (col1, col2)
        for col1, col2 in zip(test_data_col0, test_data_col0)]
    assert data == expected_data


def test_join_type(
        db, saved_dataset_no_dsrc, saved_connection,
        default_service_registry,
        default_async_service_registry,
        default_sync_usm,
):
    connection = saved_connection
    dataset = saved_dataset_no_dsrc
    sync_service_registry = default_service_registry
    us_manager = default_sync_usm
    ds_wrapper = EditableDatasetTestWrapper(dataset=dataset, us_manager=us_manager)

    table_1 = make_table(db, start_value=0, rows=10)
    table_2 = make_table(db, start_value=5, rows=10)  # so that they intersect at rows 5-9

    table_1_col0 = list(range(10))
    table_2_col0 = list(range(5, 15))
    expected_intersection = sorted(set(table_1_col0) & set(table_2_col0))

    this_dsrc_id = add_source(
        dataset=dataset, connection=connection,
        db=db, table_name=table_1.name,
        us_manager=us_manager, service_registry=sync_service_registry,
    )
    other_dsrc_id = add_source(
        dataset=dataset, connection=connection,
        db=db, table_name=table_2.name,
        us_manager=us_manager, service_registry=sync_service_registry,
    )

    avatar_1_id = str(uuid.uuid4())
    ds_wrapper.add_avatar(avatar_id=avatar_1_id, source_id=this_dsrc_id, title='This')
    avatar_2_id = str(uuid.uuid4())
    ds_wrapper.add_avatar(avatar_id=avatar_2_id, source_id=other_dsrc_id, title='Other')

    relation_id = str(uuid.uuid4())
    ds_wrapper.add_avatar_relation(
        relation_id=relation_id,
        left_avatar_id=avatar_1_id, right_avatar_id=avatar_2_id,
        conditions=[
            BinaryCondition(
                operator=BinaryJoinOperator.eq,
                left_part=ConditionPartDirect(source='int_value'),
                right_part=ConditionPartDirect(source='int_value'),
            )
        ]
    )

    def select_for_join_type(join_type: JoinType) -> list:
        ds_wrapper.update_avatar_relation(relation_id=relation_id, join_type=join_type)
        role = DataSourceRole.origin
        join_on_expressions = [
            JoinOnExpressionCtx(
                expression=col(ds_wrapper, avatar_1_id, 'int_value') == col(ds_wrapper, avatar_2_id, 'int_value'),
                avatar_ids=[avatar_1_id, avatar_2_id],
                user_type=BIType.boolean,
                left_id=avatar_1_id,
                right_id=avatar_2_id,
                join_type=join_type,
            ),
        ]
        root_avatar_id = avatar_1_id
        required_avatar_ids = [avatar_1_id, avatar_2_id]
        # dialect-dependent formula for sorting values
        ifnull = {
            CONNECTION_TYPE_MYSQL: sa.func.IFNULL,
            ConnectionType.postgres: sa.func.COALESCE,
            CONNECTION_TYPE_MSSQL: sa.func.ISNULL,
            ConnectionType.clickhouse: sa.func.ifNull,
            CONNECTION_TYPE_ORACLE: sa.func.NVL,
        }[db.conn_type]
        if db.conn_type == CONNECTION_TYPE_MSSQL:
            greatest = lambda x, y: sa.func.IIF(x >= y, x, y)  # noqa
        else:
            greatest = lambda x, y: sa.func.greatest(ifnull(x, 0), ifnull(y, 0))  # noqa

        bi_query = BIQuery(
            select_expressions=[
                ExpressionCtx(
                    expression=col(ds_wrapper, avatar_1_id, 'int_value'),
                    avatar_ids=[avatar_1_id],
                    alias='col1',
                    user_type=BIType.integer,
                ),
                ExpressionCtx(
                    expression=col(ds_wrapper, avatar_2_id, 'int_value'),
                    avatar_ids=[avatar_2_id],
                    alias='col2',
                    user_type=BIType.integer,
                ),
            ],
            order_by_expressions=[
                OrderByExpressionCtx(
                    expression=greatest(
                        col(ds_wrapper, avatar_1_id, 'int_value'),
                        col(ds_wrapper, avatar_2_id, 'int_value')
                    ),
                    avatar_ids=[avatar_1_id, avatar_2_id],
                    user_type=BIType.integer,
                ),
            ],
        )
        data_fetcher = DataFetcher(
            service_registry=default_async_service_registry,
            dataset=dataset, us_manager=us_manager,
        )
        data = list(data_fetcher.get_data_stream(
            role=role,
            bi_query=bi_query,
            root_avatar_id=root_avatar_id,
            required_avatar_ids=required_avatar_ids,
            join_on_expressions=join_on_expressions,
        ).data)
        return [row[0] for row in data], [row[1] for row in data]

    not_found = None

    i_1_values, i_2_values = select_for_join_type(join_type=JoinType.inner)
    assert i_1_values == i_2_values == expected_intersection

    i_1_values, i_2_values = select_for_join_type(join_type=JoinType.left)
    assert i_1_values == table_1_col0
    assert i_2_values == [not_found] * 5 + expected_intersection

    if db.conn_type in (ConnectionType.clickhouse, ConnectionType.postgres, CONNECTION_TYPE_MYSQL):
        i_1_values, i_2_values = select_for_join_type(join_type=JoinType.right)
        assert i_1_values == expected_intersection + [not_found] * 5
        assert i_2_values == table_2_col0
    else:
        with pytest.raises(exc.DatasetConfigurationError):
            select_for_join_type(join_type=JoinType.right)

    if db.conn_type in (ConnectionType.clickhouse, ConnectionType.postgres):
        i_1_values, i_2_values = select_for_join_type(join_type=JoinType.full)
        assert i_1_values == table_1_col0 + [not_found] * 5
        assert i_2_values == [not_found] * 5 + table_2_col0
    else:
        with pytest.raises(exc.DatasetConfigurationError):
            select_for_join_type(join_type=JoinType.full)


def test_update_data_source_collection(saved_ch_dataset, default_sync_usm):
    us_manager = default_sync_usm
    dataset = saved_ch_dataset
    ds_wrapper = EditableDatasetTestWrapper(dataset=dataset, us_manager=us_manager)
    source_id = dataset.get_single_data_source_id()
    ds_wrapper.update_data_source_collection(source_id=source_id, title='My Data Source')
    dsrc_coll_spec = ds_wrapper.get_data_source_coll_strict(source_id=source_id)
    assert dsrc_coll_spec.title == 'My Data Source'


def test_resolve_role(
        saved_dataset_no_dsrc, two_clickhouse_tables, saved_ch_connection,
        default_sync_usm, default_service_registry,
):
    service_registry = default_service_registry
    us_manager = default_sync_usm
    dataset = saved_dataset_no_dsrc
    ds_wrapper = EditableDatasetTestWrapper(dataset=dataset, us_entry_buffer=us_manager.get_entry_buffer())
    table_1, table_2 = two_clickhouse_tables

    # not materialized, no sources
    assert ds_wrapper.resolve_source_role() == DataSourceRole.origin
    assert ds_wrapper.resolve_source_role(for_preview=True) == DataSourceRole.origin

    info = patch_dataset_with_two_sources(
        dataset=dataset, us_manager=us_manager, service_registry=service_registry,
        table_1=table_1, table_2=table_2, connection=saved_ch_connection,
    )

    # not materialized, no samples
    assert ds_wrapper.resolve_source_role() == DataSourceRole.origin
    assert ds_wrapper.resolve_source_role(for_preview=True) == DataSourceRole.origin

    # not materialized, sample for first source
    ds_wrapper.add_data_source(
        source_id=info.source_ids[0], role=DataSourceRole.sample, created_from=CreateDSFrom.CH_TABLE,
        parameters=dict(db_name=table_1.db.name, table_name=table_1.name))
    assert ds_wrapper.resolve_source_role() == DataSourceRole.origin
    assert ds_wrapper.resolve_source_role(for_preview=True) == DataSourceRole.origin

    # not materialized, samples for both sources
    ds_wrapper.add_data_source(
        source_id=info.source_ids[1], role=DataSourceRole.sample, created_from=CreateDSFrom.CH_TABLE,
        parameters=dict(db_name=table_2.db.name, table_name=table_2.name))

    assert ds_wrapper.resolve_source_role() == DataSourceRole.origin
    assert ds_wrapper.resolve_source_role(for_preview=True) == DataSourceRole.origin  # because sample has been disabled


def test_select_data_use_subquery(
        default_sync_usm,
        two_tables,
        saved_dataset_no_dsrc,
        saved_connection,
        default_service_registry,
        default_async_service_registry,
):
    sync_service_registry = default_service_registry
    dataset = saved_dataset_no_dsrc
    us_manager = default_sync_usm
    ds_wrapper = DatasetTestWrapper(dataset=dataset, us_manager=us_manager)
    connection = saved_connection
    table_1, table_2 = two_tables
    info = patch_dataset_with_two_sources(
        dataset=dataset,
        us_manager=us_manager,
        service_registry=sync_service_registry,
        table_1=table_1, table_2=table_2,
        connection=connection, fill_raw_schema=True,
    )
    join_on_expressions = [
        JoinOnExpressionCtx(
            expression=(
                col(ds_wrapper, info.avatar_ids[0], 'int_value') == col(ds_wrapper, info.avatar_ids[1], 'int_value')
            ),
            user_type=BIType.boolean,
            avatar_ids=info.avatar_ids,
            left_id=info.avatar_ids[0],
            right_id=info.avatar_ids[1],
            join_type=JoinType.inner,
        ),
    ]

    root_avatar_id = info.avatar_ids[0]
    required_avatar_ids = info.avatar_ids
    role = DataSourceRole.origin

    # reload
    us_manager.save(dataset)
    dataset = us_manager.get_by_id(dataset.uuid)
    us_manager.load_dependencies(dataset)
    ds_wrapper = DatasetTestWrapper(dataset=dataset, us_manager=us_manager)

    expr_ctx_1 = ExpressionCtx(
        expression=col(ds_wrapper, info.avatar_ids[0], 'datetime_value'),
        user_type=BIType.datetime, avatar_ids=[info.avatar_ids[0]], alias=str(uuid.uuid4()),
    )
    expr_ctx_2 = ExpressionCtx(
        expression=col(ds_wrapper, info.avatar_ids[1], 'datetime_value'),
        user_type=BIType.datetime, avatar_ids=[info.avatar_ids[1]], alias=str(uuid.uuid4()),
    )

    bi_query = BIQuery(
        select_expressions=[expr_ctx_1, expr_ctx_2],
        order_by_expressions=[
            attrs_evolve_to_subclass(cls=OrderByExpressionCtx, inst=expr_ctx_1),
            attrs_evolve_to_subclass(cls=OrderByExpressionCtx, inst=expr_ctx_2),
        ],
        group_by_expressions=[expr_ctx_1, expr_ctx_2],
    )
    data_fetcher = DataFetcher(
        service_registry=default_async_service_registry,
        dataset=dataset, us_manager=us_manager,
    )
    data = list(data_fetcher.get_data_stream(
        role=role,
        bi_query=bi_query,
        root_avatar_id=root_avatar_id,
        required_avatar_ids=required_avatar_ids,
        join_on_expressions=join_on_expressions,
        from_subquery=True,
        subquery_limit=7,
    ).data)
    # Because it is an inner join, and subqueries are `limit`ed separately, the
    # resulting intersection can be smaller.
    assert len(data) <= 7


def test_manage_obligatory_filters(default_sync_usm, saved_ch_dataset, app_request_context):
    us_manager = default_sync_usm
    dataset = us_manager.get_by_id(saved_ch_dataset.uuid, expected_type=Dataset)
    ds_wrapper = EditableDatasetTestWrapper(dataset=dataset, us_manager=us_manager)
    column = SchemaColumn(
        name='int_value',
        title='int_value',
        user_type=BIType.integer,
        nullable=True,
        native_type=ClickHouseNativeType.normalize_name_and_create(
            conn_type=ConnectionType.clickhouse, name='int64'),
        source_id=dataset.get_single_data_source_id(),
    )
    field_data = dataset.create_result_schema_field(column=column)
    ds_wrapper.set_result_schema(ResultSchema([BIField.make(**field_data)]))
    field = dataset.result_schema.fields[0]
    filter_id = str(uuid.uuid4())
    ds_wrapper.add_obligatory_filter(
        obfilter_id=filter_id,
        field_guid=field.guid,
        default_filters=[DefaultWhereClause(operation=WhereClauseOperation.EQ, values=[0])],
        managed_by=ManagedBy.user,
        valid=False,
    )

    filters = ds_wrapper.get_obligatory_filter_list()
    assert len(filters) == 1
    assert filters[0].id == filter_id
    assert filters[0].field_guid == field.guid
    assert filters[0].valid is False
    assert len(filters[0].default_filters) == 1
    assert filters[0].default_filters[0] == DefaultWhereClause(operation=WhereClauseOperation.EQ, values=[0])
    assert ds_wrapper.get_obligatory_filter_strict(obfilter_id=filter_id) == filters[0]
    assert ds_wrapper.get_obligatory_filter_strict(field_guid=field.guid) == filters[0]

    ds_wrapper.update_obligatory_filter(
        obfilter_id=filter_id,
        default_filters=[
            DefaultWhereClause(operation=WhereClauseOperation.GT, values=[0]),
            DefaultWhereClause(operation=WhereClauseOperation.NE, values=[100]),
        ],
    )
    filters = ds_wrapper.get_obligatory_filter_list()
    assert filters[0].id == filter_id
    assert filters[0].field_guid == field.guid
    assert len(filters[0].default_filters) == 2
    assert filters[0].default_filters[0] == DefaultWhereClause(operation=WhereClauseOperation.GT, values=[0])
    assert filters[0].valid is False

    ds_wrapper.update_obligatory_filter(obfilter_id=filter_id, valid=True)
    filters = ds_wrapper.get_obligatory_filter_list()
    assert len(filters[0].default_filters) == 2
    assert filters[0].valid

    ds_wrapper.remove_obligatory_filter(obfilter_id=filter_id)
    filters = ds_wrapper.get_obligatory_filter_list()
    assert len(filters) == 0
    assert ds_wrapper.get_obligatory_filter_opt(field_guid=field.guid) is None


# TODO FIX: Add some data source links to ensure that it are not deleted during copy
def test_dataset_copy(
        default_sync_usm,
        two_tables,
        saved_dataset_no_dsrc,
        saved_connection,
        default_service_registry,
        default_async_service_registry,
):
    sync_service_registry = default_service_registry
    dataset = saved_dataset_no_dsrc
    us_manager = default_sync_usm

    patch_dataset_with_two_sources(
        dataset=dataset,
        us_manager=us_manager,
        service_registry=sync_service_registry,
        connection=saved_connection,
        table_1=two_tables[0], table_2=two_tables[1],
        fill_raw_schema=True,
    )

    orig_key = dataset.entry_key
    copy_key: EntryLocation

    if isinstance(orig_key, PathEntryLocation):
        copy_key = PathEntryLocation(
            path=f"{orig_key.path} (copy {shortuuid.uuid()})",
        )
    elif isinstance(orig_key, WorkbookEntryLocation):
        copy_key = WorkbookEntryLocation(
            workbook_id=orig_key.workbook_id,
            entry_name=f"{orig_key.entry_name} (copy {shortuuid.uuid()})",
        )
    else:
        raise AssertionError(f"Unsupported type of orig key: {orig_key}")

    dataset_copy = us_manager.copy_entry(dataset, key=copy_key)

    assert isinstance(dataset_copy, Dataset)
    assert dataset_copy is not dataset

    us_manager.save(dataset_copy)

    us_manager.delete(dataset_copy)


def test_dataset_created_via(default_sync_usm, saved_dataset, yt_to_dl_dataset):
    us_manager = default_sync_usm
    dataset = us_manager.get_by_id(saved_dataset.uuid, expected_type=Dataset)
    yt_to_dl_dataset = us_manager.get_by_id(yt_to_dl_dataset.uuid, expected_type=Dataset)

    assert dataset.created_via == DataSourceCreatedVia.user
    assert yt_to_dl_dataset.created_via == DataSourceCreatedVia.yt_to_dl


def test_data_source_with_idx(saved_dataset_no_dsrc, saved_connection, default_sync_usm):
    conn = saved_connection
    ds = saved_dataset_no_dsrc
    us_manager = default_sync_usm

    ds_wrapper = EditableDatasetTestWrapper(dataset=ds, us_manager=us_manager)
    source_id = shortuuid.uuid()

    parameters = dict(table_name='fake_table_name')
    if conn.conn_type == ConnectionType.clickhouse:
        parameters['db_name'] = 'fake_db_name'
    if conn.has_schema:
        parameters['schema_name'] = 'fake_schema_name'
    original_idx_info_set = frozenset([
        IndexInfo(columns=('a', 'b'), kind=IndexKind.table_sorting),
        IndexInfo(columns=('b',), kind=None),
    ])

    ds_wrapper.add_data_source(
        source_id=source_id, role=DataSourceRole.origin,
        created_from=SOURCE_TYPE_BY_CONN_TYPE[saved_connection.conn_type],
        connection_id=saved_connection.uuid,
        parameters=parameters,
        index_info_set=original_idx_info_set,
    )
    us_manager.save(ds)
    ds = us_manager.get_by_id(ds.uuid, Dataset)
    ds_wrapper = EditableDatasetTestWrapper(dataset=ds, us_manager=us_manager)
    # Check that indexes info was correctly saved (exactly test correct serialization of index info)
    reloaded_idx_info_set = ds_wrapper.get_data_source_strict(
        source_id=source_id, role=DataSourceRole.origin).saved_index_info_set
    assert reloaded_idx_info_set == original_idx_info_set

    # Update data source index info list with None ...
    ds_wrapper.update_data_source(source_id=source_id, role=DataSourceRole.origin, index_info_set=None)
    # ... and ensure it was removed
    assert ds_wrapper.get_data_source_strict(source_id, DataSourceRole.origin).saved_index_info_set is None
    # ... even after reload
    us_manager.save(ds)
    ds = us_manager.get_by_id(ds.uuid, Dataset)
    ds_wrapper = EditableDatasetTestWrapper(dataset=ds, us_manager=us_manager)
    reloaded_idx_info_set = ds_wrapper.get_data_source_strict(
        source_id=source_id, role=DataSourceRole.origin).saved_index_info_set
    assert reloaded_idx_info_set is None

    # And restore index info
    ds_wrapper.update_data_source(source_id=source_id, role=DataSourceRole.origin, index_info_set=original_idx_info_set)
    us_manager.save(ds)
    ds = us_manager.get_by_id(ds.uuid, Dataset)
    ds_wrapper = EditableDatasetTestWrapper(dataset=ds, us_manager=us_manager)
    # Check that indexes info was correctly saved
    reloaded_idx_info_set = ds_wrapper.get_data_source_strict(
        source_id=source_id, role=DataSourceRole.origin).saved_index_info_set
    assert reloaded_idx_info_set == original_idx_info_set
