from typing import (
    Optional,
    Type,
)

from clickhouse_sqlalchemy import types as ch_types
from clickhouse_sqlalchemy.drivers.base import ClickHouseDialect
from clickhouse_sqlalchemy.ext.clauses import Lambda
import sqlalchemy as sa
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql.selectable import Select

from dl_api_commons.base_models import RequestContextInfo
from dl_connector_clickhouse.core.clickhouse.testing.exec_factory import ClickHouseExecutorFactory
from dl_connector_clickhouse.core.clickhouse_base.adapters import (
    AsyncClickHouseAdapter,
    ClickHouseAdapter,
)
from dl_connector_postgresql.core.postgresql.testing.exec_factory import PostgresExecutorFactory
from dl_connector_postgresql.core.postgresql_base.adapters_postgres import PostgresAdapter
from dl_connector_postgresql.core.postgresql_base.async_adapters_postgres import AsyncPostgresAdapter
from dl_core.connection_executors.adapters.common_base import CommonBaseDirectAdapter
from dl_core.connection_executors.models.db_adapter_data import DBAdapterQuery
from dl_core.connection_executors.qe_serializer.dba_actions import ActionExecuteQuery
from dl_core.connection_executors.qe_serializer.serializer import ActionSerializer
from dl_core_testing.executors import ExecutorFactoryBase
from dl_formula.definitions.common import within_group

SUPPORTED_ADAPTER_CLS: frozenset[Type[CommonBaseDirectAdapter]] = frozenset(
    {
        ClickHouseAdapter,
        AsyncClickHouseAdapter,
        PostgresAdapter,
        AsyncPostgresAdapter,
    }
)


def _check_action_serialization_deserialization(
    action: ActionExecuteQuery, dialect: Optional[DefaultDialect] = None
) -> None:
    serializer = ActionSerializer()
    serialized_action = serializer.serialize_action(action)
    deserialized_action = serializer.deserialize_action(serialized_action, allowed_dba_classes=SUPPORTED_ADAPTER_CLS)
    assert isinstance(deserialized_action, ActionExecuteQuery)

    # Direct comparison of action objects doesn't work because SA queries never equal one another.
    # So just compare the stringified queries.
    deserialized_query = str(
        deserialized_action.db_adapter_query.query.compile(dialect=dialect, compile_kwargs={"literal_binds": True})
    )
    original_query = str(action.db_adapter_query.query.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))
    assert deserialized_query == original_query


def _make_query_action(exec_factory: ExecutorFactoryBase, sa_query: Select) -> ActionExecuteQuery:
    action = ActionExecuteQuery(
        target_conn_dto=exec_factory.make_dto(),
        dba_cls=exec_factory.get_dba_class(),
        req_ctx_info=RequestContextInfo(),
        db_adapter_query=DBAdapterQuery(
            query=sa_query,
        ),
    )
    return action


def test_postgres_query_within_group(postgres_db):
    action = _make_query_action(
        exec_factory=PostgresExecutorFactory(db=postgres_db),
        sa_query=sa.select(
            [
                within_group(
                    sa.func.percentile_cont(sa.literal(1)),
                    sa.literal(1),
                ),
            ]
        ),
    )
    _check_action_serialization_deserialization(action)


def test_postgres_query_within_group_over(postgres_db):
    action = _make_query_action(
        exec_factory=PostgresExecutorFactory(db=postgres_db),
        sa_query=sa.select(
            [
                within_group(
                    sa.func.percentile_cont(sa.literal(1)),
                    sa.literal(1),
                ).over(),
            ]
        ),
    )
    _check_action_serialization_deserialization(action)


def test_clickhouse_lambda(clickhouse_db):
    action = _make_query_action(
        exec_factory=ClickHouseExecutorFactory(db=clickhouse_db),
        sa_query=sa.select(
            [
                sa.func.arrayStringConcat(
                    sa.func.arrayMap(
                        Lambda(lambda x: sa.cast(x, ch_types.String)),
                        sa.func.arrayFilter(Lambda(lambda x: sa.func.isNotNull(x)), sa.literal([1, None, 2])),
                    ),
                    ",",
                )
            ]
        ),
    )
    _check_action_serialization_deserialization(action, dialect=ClickHouseDialect())
