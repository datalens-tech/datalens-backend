from __future__ import annotations

import datetime
import logging
import uuid
from typing import TYPE_CHECKING, Any, Iterable, List, NamedTuple, Optional, Tuple

import sqlalchemy as sa
from sqlalchemy.sql.elements import ColumnClause

from bi_testing.utils import get_log_record

from bi_constants.enums import BinaryJoinOperator, ConnectionType, CreateDSFrom, DataSourceRole, JoinType

from bi_api_commons.reporting.profiler import PROFILING_LOG_NAME, QUERY_PROFILING_ENTRY
from bi_core.base_models import DefaultConnectionRef
from bi_core.multisource import BinaryCondition, ConditionPartDirect
from bi_core_testing.database import Db, DbTable, make_db
from bi_core.us_connection_base import ClassicConnectionSQL
from bi_core.us_dataset import Dataset
from bi_core.us_manager.us_manager_sync import SyncUSManager
from bi_core.components.editor import DatasetComponentEditor
from bi_core_testing.dataset_wrappers import DatasetTestWrapper, EditableDatasetTestWrapper
from bi_core.services_registry.top_level import ServicesRegistry

from bi_connector_mssql.core.constants import CONNECTION_TYPE_MSSQL, SOURCE_TYPE_MSSQL_TABLE
from bi_connector_mysql.core.constants import CONNECTION_TYPE_MYSQL, SOURCE_TYPE_MYSQL_TABLE
from bi_connector_oracle.core.constants import CONNECTION_TYPE_ORACLE, SOURCE_TYPE_ORACLE_TABLE
from bi_connector_postgresql.core.postgresql.constants import CONNECTION_TYPE_POSTGRES, SOURCE_TYPE_PG_TABLE

from bi_legacy_test_bundle_tests.core import config

if TYPE_CHECKING:
    from bi_core.us_connection_base import ConnectionBase


SOURCE_TYPE_BY_CONN_TYPE = {
    ConnectionType.clickhouse: CreateDSFrom.CH_TABLE,
    CONNECTION_TYPE_POSTGRES: SOURCE_TYPE_PG_TABLE,
    CONNECTION_TYPE_MYSQL: SOURCE_TYPE_MYSQL_TABLE,
    CONNECTION_TYPE_MSSQL: SOURCE_TYPE_MSSQL_TABLE,
    CONNECTION_TYPE_ORACLE: SOURCE_TYPE_ORACLE_TABLE,
}

# DB_CLICKHOUSE_CLUSTER = config.DB_CLICKHOUSE_CLUSTER
OTHER_DBS = {
    ConnectionType[name]: url
    for name, url in config.OTHER_DBS.items()
}


def get_other_db(conn_type: ConnectionType, bi_config=None) -> Optional[Db]:
    if bi_config is not None:
        url, cluster = bi_config.OTHER_DBS[conn_type.value]
    else:
        url, cluster = OTHER_DBS[conn_type]
    if not url:
        return None
    return make_db(url=url, conn_type=conn_type, cluster=cluster)


def _is_profiling_record(rec) -> bool:
    return (
        rec.name == PROFILING_LOG_NAME
        and rec.msg == QUERY_PROFILING_ENTRY
    )


def get_dump_request_profile_records(caplog, single: bool = False):
    return get_log_record(
        caplog,
        predicate=_is_profiling_record,
        single=single,
    )


def assert_no_warnings(caplog):
    for record in caplog.records:
        assert record.levelno < logging.WARNING, \
            f'Found a record with warning (or higher) level in log: {record}'


def add_source(
        dataset: Dataset, connection: ConnectionBase, db: Db,
        table_name: str,
        *,
        service_registry: ServicesRegistry,
        us_manager: SyncUSManager,
        schema_name: Optional[str] = None,
        id: Optional[str] = None,
        role: DataSourceRole = DataSourceRole.origin,
        fill_raw_schema: bool = True,
        data_dump_id: Optional[str] = None,
):
    source_id: str = id or str(uuid.uuid4())
    ds_wrapper = EditableDatasetTestWrapper(dataset=dataset, us_entry_buffer=us_manager.get_entry_buffer())
    parameters = dict(table_name=table_name)
    if isinstance(connection, ClassicConnectionSQL):
        # clickhouse connections have no db_name, so that has to be specified in the dataset
        if connection.conn_type == ConnectionType.clickhouse:
            parameters['db_name'] = db.name
        if connection.has_schema:
            parameters['schema_name'] = schema_name
    if data_dump_id is not None:
        parameters['data_dump_id'] = data_dump_id

    us_manager.ensure_entry_preloaded(DefaultConnectionRef(conn_id=connection.uuid))
    ds_wrapper.add_data_source(
        source_id=source_id, role=role, created_from=SOURCE_TYPE_BY_CONN_TYPE[connection.conn_type],
        connection_id=connection.uuid,
        parameters=parameters,
    )
    if fill_raw_schema:
        dsrc = ds_wrapper.get_data_source_strict(source_id=source_id, role=role)
        conn_executor = service_registry.get_conn_executor_factory().get_sync_conn_executor(conn=dsrc.connection)
        raw_schema = dsrc.get_schema_info(conn_executor_factory=lambda: conn_executor).schema
        assert raw_schema is not None
        ds_wrapper.update_data_source(source_id=source_id, role=role, raw_schema=raw_schema)
    return source_id


def col(ds_wrapper: DatasetTestWrapper, *parts) -> ColumnClause:
    return sa.literal_column('.'.join([
        ds_wrapper.quote(part, role=DataSourceRole.origin)
        for part in parts
    ]))


class MultisourceInfo(NamedTuple):
    source_ids: List[str]
    avatar_ids: List[str]
    relation_ids: List[str]


def patch_dataset_with_two_sources(
        dataset: Dataset,
        service_registry: ServicesRegistry,
        us_manager: SyncUSManager,
        table_1: DbTable, table_2: DbTable,
        connection: ConnectionBase,
        second_connection: Optional[ConnectionBase] = None,
        fill_raw_schema: bool = False,
) -> MultisourceInfo:
    # TODO: Generalize for arbitrary number of tables, connections and custom relations
    """ Add two sources from a single or two different connections to an empty dataset. """
    ds_editor = DatasetComponentEditor(dataset=dataset)

    second_connection = second_connection if second_connection is not None else connection
    source_1_id = add_source(
        dataset=dataset, connection=connection, db=table_1.db,
        table_name=table_1.name, fill_raw_schema=fill_raw_schema,
        us_manager=us_manager, service_registry=service_registry,
    )
    source_2_id = add_source(
        dataset=dataset, connection=second_connection, db=table_2.db,
        table_name=table_2.name, fill_raw_schema=fill_raw_schema,
        us_manager=us_manager, service_registry=service_registry,
    )

    avatar_1_id = str(uuid.uuid4())
    ds_editor.add_avatar(avatar_id=avatar_1_id, source_id=source_1_id, title='First')
    avatar_2_id = str(uuid.uuid4())
    ds_editor.add_avatar(avatar_id=avatar_2_id, source_id=source_2_id, title='Second')

    relation_id = str(uuid.uuid4())
    ds_editor.add_avatar_relation(
        relation_id=relation_id,
        left_avatar_id=avatar_1_id, right_avatar_id=avatar_2_id,
        join_type=JoinType.inner,
        conditions=[
            BinaryCondition(
                operator=BinaryJoinOperator.eq,
                left_part=ConditionPartDirect(source='int_value'),
                right_part=ConditionPartDirect(source='int_value'),
            )
        ]
    )
    return MultisourceInfo(
        source_ids=[source_1_id, source_2_id],
        avatar_ids=[avatar_1_id, avatar_2_id],
        relation_ids=[relation_id],
    )


def simple_groupby(lst: Iterable[Tuple[Any, Any]]):
    """
    >>> simple_groupby([(1, 1), (2, 2), (1, 3)])
    {1: [1, 3], 2: [2]}
    """
    res = {}
    for key, val in lst:
        try:
            group_list = res[key]
        except KeyError:
            res[key] = [val]
        else:
            group_list.append(val)
    return res


def almost_equal(value_1, value_2, **kwargs):
    if value_1 == value_2:
        return True

    # ORA case: value_1 is a naive day-start datetime
    if (isinstance(value_1, datetime.datetime) and isinstance(value_2, datetime.date) and
            kwargs.get('lenient_date') and
            value_1.hour == 0 and value_1.minute == 0 and value_1.second == 0 and value_1.microsecond == 0 and
            value_1.tzinfo is None):
        return True

    # ORA case: int as bool
    if (isinstance(value_1, int) and isinstance(value_2, bool) and
            kwargs.get('lenient_bool') and
            value_1 in (0, 1) and bool(value_1) == value_2):
        return True

    if type(value_1) is not type(value_2):
        return False

    if isinstance(value_1, (list, tuple)):
        return len(value_1) == len(value_2) and all(
            almost_equal(value_1[i], value_2[i], **kwargs)
            for i in range(len(value_1))
        )
    if isinstance(value_1, dict):
        return len(value_1) == len(value_2) and all(
            almost_equal(value_1[key], value_2[key], **kwargs)
            for key in value_1
        )
    if isinstance(value_1, datetime.datetime):
        return abs((value_1 - value_2).total_seconds()) <= kwargs.get('seconds', 1)

    return False


def make_equal_if_almost(value_1, value_2, **kwargs):
    """
    Make `value_1` equal to `value_2` where it is `almost_equal`,
    recursively.

    Useful for seeing diffs in test results.
    """
    if value_1 == value_2:
        return value_1

    if type(value_1) is not type(value_2):
        return value_1  # inconvergible

    if isinstance(value_1, (list, tuple)):
        result = [
            make_equal_if_almost(item_1, item_2, **kwargs)
            for item_1, item_2 in zip(value_1, value_2)]
        result += value_1[len(result):]  # if value_1 was longer
        return type(value_1)(result)

    if isinstance(value_1, dict):
        result = {
            key: make_equal_if_almost(
                val, value_2.get(key), **kwargs)
            for key, val in value_1.items()}
        return result

    if almost_equal(value_1, value_2, **kwargs):
        return value_2

    return value_1
