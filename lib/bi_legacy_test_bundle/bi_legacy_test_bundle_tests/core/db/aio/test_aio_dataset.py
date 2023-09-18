from __future__ import annotations

import logging
import uuid
from datetime import date

import pytest
import sqlalchemy as sa
from sqlalchemy.sql.elements import ClauseElement

from dl_constants.enums import BIType, DataSourceRole, ManagedBy

from dl_api_commons.reporting.profiler import DefaultReportingProfiler

from dl_core import exc
from dl_connector_postgresql.core.postgresql_base.type_transformer import PostgreSQLTypeTransformer
from dl_connector_postgresql.core.postgresql.us_connection import ConnectionPostgreSQL
from dl_core.data_processing.cache.engine import EntityCacheEngineAsync
from dl_core.db.elements import SchemaColumn
from dl_core.query.bi_query import BIQuery
from dl_core.query.expression import ExpressionCtx, OrderByExpressionCtx
from dl_core.services_registry import ServicesRegistry
from dl_core_testing.data import DataFetcher
from dl_core_testing.database import C, Db, make_table
from dl_core_testing.utils import SROptions
from dl_core.us_dataset import Dataset
from dl_core.us_manager.us_manager_async import AsyncUSManager
from dl_core.utils import attrs_evolve_to_subclass
from dl_core.components.editor import DatasetComponentEditor

from dl_connector_postgresql.core.postgresql.constants import SOURCE_TYPE_PG_TABLE

from bi_legacy_test_bundle_tests.core.utils import get_dump_request_profile_records, assert_no_warnings


PG_TT = PostgreSQLTypeTransformer()


def _make_schema_column(name: str, user_type: BIType) -> SchemaColumn:
    return SchemaColumn(
        name=name,
        title=name,
        user_type=user_type,
        native_type=PG_TT.type_user_to_native(user_type),
    )


@pytest.mark.asyncio
async def test_dataset_async_select_data(
        saved_dataset_no_dsrc: Dataset,
        postgres_db: Db,
        clickhouse_db: Db,
        saved_pg_connection: ConnectionPostgreSQL,
        local_private_usm: AsyncUSManager,
        async_service_registry_factory,
        bi_context,
        caplog,
):
    us_manager = local_private_usm
    dataset = saved_dataset_no_dsrc
    ds_editor = DatasetComponentEditor(dataset=dataset)
    async_sr_with_caches: ServicesRegistry = async_service_registry_factory(SROptions(rci=bi_context, with_caches=True))
    caplog.set_level(logging.INFO, logger='bi_core.profiling_db')
    caplog.clear()

    data = [
        (date(2019, 1, 1), "EVT_1", 141),
        (date(2019, 1, 1), "EVT_2", 143),
        (date(2019, 1, 1), "EVT_3", 146),
        (date(2019, 1, 1), "EVT_4", 149),
        (date(2019, 1, 2), "EVT_1", 143),
        (date(2019, 1, 2), "EVT_2", 134),
        (date(2019, 1, 2), "EVT_3", 163),
        (date(2019, 1, 2), "EVT_4", 324),
        (date(2019, 1, 2), "EVT_5", 142),
        (date(2019, 1, 3), "EVT_1", 554),
        (date(2019, 1, 3), "EVT_2", 725),
        (date(2019, 1, 3), "EVT_3", 245),
        (date(2019, 1, 3), "EVT_4", 749),
        (date(2019, 1, 4), "EVT_1", 953),
        (date(2019, 1, 4), "EVT_2", 334),
        (date(2019, 1, 5), "EVT_5", 334),
    ]
    data.sort(key=lambda o: (o[0], o[1]))

    cg = C.array_data_getter(data)
    columns = [
        C("date", BIType.date, vg=cg[0]),
        C("event_code", BIType.string, vg=cg[1]),
        C("event_count", BIType.integer, vg=cg[2]),
    ]
    raw_schema = [
        _make_schema_column('date', BIType.date),
        _make_schema_column('event_code', BIType.string),
        _make_schema_column('event_count', BIType.integer),
    ]

    orig_table = make_table(
        postgres_db,
        rows=len(data),
        columns=columns,
    )

    dsrc_id = uuid.uuid4().hex

    ds_editor.add_data_source(
        source_id=dsrc_id,
        created_from=SOURCE_TYPE_PG_TABLE,
        connection_id=saved_pg_connection.uuid,
        managed_by=ManagedBy.user,
        raw_schema=raw_schema,
        parameters=dict(
            db_name=postgres_db.name, table_name=orig_table.name,
        ),
    )
    avatar_id = uuid.uuid4().hex
    ds_editor.add_avatar(avatar_id=avatar_id, source_id=dsrc_id, title='My Avatar')
    await us_manager.save(dataset)

    def make_expr(expression: ClauseElement, alias: str, user_type: BIType) -> ExpressionCtx:
        return ExpressionCtx(
            expression=expression,
            avatar_ids=[avatar_id],
            alias=alias, user_type=user_type,
        )

    dataset = await us_manager.get_by_id(dataset.uuid, Dataset)
    await us_manager.load_dependencies(dataset)

    dce = EntityCacheEngineAsync(
        entity_id=dataset.uuid,
        rc=async_sr_with_caches.get_caches_redis_client(),
    )
    await dce.invalidate_all()
    bi_query = BIQuery(
        select_expressions=[
            make_expr(sa.literal_column("date"), 'date', BIType.date),
            make_expr(sa.literal_column("event_code"), 'event_code', BIType.string),
            make_expr(sa.literal_column("event_count"), 'event_count', BIType.integer),
        ],
        order_by_expressions=[
            attrs_evolve_to_subclass(
                cls=OrderByExpressionCtx,
                inst=make_expr(sa.literal_column("date"), 'date', BIType.date),
            ),
            attrs_evolve_to_subclass(
                cls=OrderByExpressionCtx,
                inst=make_expr(sa.literal_column("event_code"), 'event_code', BIType.string),
            ),
        ],
    )

    role = DataSourceRole.origin

    # Expecting no cache hit
    data_fetcher = DataFetcher(
        service_registry=async_sr_with_caches,
        dataset=dataset, us_manager=us_manager,
    )
    result = await (await data_fetcher.get_data_stream_async(
        role=role,
        bi_query=bi_query,
    )).data.all()
    reporting_registry = async_sr_with_caches.get_reporting_registry()
    profiler = DefaultReportingProfiler(reporting_registry=reporting_registry)
    profiler.flush_all_query_reports()
    reporting_registry.clear_records()

    assert result == data
    log_record = get_dump_request_profile_records(caplog, single=True)
    assert log_record.query is not None
    assert log_record.cache_used is True
    assert log_record.cache_full_hit is False
    assert_no_warnings(caplog)
    caplog.clear()

    # Expecting cache hit
    await data_fetcher.get_data_stream_async(
        role=role,
        bi_query=bi_query,
    )
    profiler.flush_all_query_reports()
    reporting_registry.clear_records()

    log_record = get_dump_request_profile_records(caplog, single=True)
    assert log_record.query is not None
    assert log_record.cache_used is True
    assert log_record.cache_full_hit is True
    assert_no_warnings(caplog)
    caplog.clear()

    # row_count_hard_limit
    with pytest.raises(exc.ResultRowCountLimitExceeded):
        await (await data_fetcher.get_data_stream_async(
            role=role,
            bi_query=bi_query,
            row_count_hard_limit=3,
        )).data.all()
