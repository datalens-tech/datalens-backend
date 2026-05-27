from typing import (
    ClassVar,
    Generic,
    TypeVar,
)

import sqlalchemy as sa
from sqlalchemy.sql.selectable import Select

from dl_core.connection_executors.models.db_adapter_data import DBAdapterQuery
from dl_core.connection_executors.models.scoped_rci import DBAdapterScopedRCI
from dl_core.connection_executors.qe_serializer.dba_actions import ActionExecuteQuery
from dl_core.connection_executors.qe_serializer.serializer import ActionSerializer
from dl_core.connection_executors.remote_query_executor.commons import (
    SUPPORTED_ADAPTER_CLS,
    register_adapter_class,
)
from dl_core.us_connection_base import ConnectionBase
from dl_core_testing.database import Db
from dl_core_testing.executors import ExecutorFactoryBase
from dl_core_testing.testcases.connection_executor import BaseConnectionExecutorTestClass

_CONN_TV = TypeVar("_CONN_TV", bound=ConnectionBase)


class DefaultQESerializerTestSuite(BaseConnectionExecutorTestClass[_CONN_TV], Generic[_CONN_TV]):
    """Suite for `ActionSerializer` round-trip with a real connector executor factory.

    Connector subclasses must declare:
    - `EXECUTOR_FACTORY_CLS`: subclass of `ExecutorFactoryBase` (e.g. `ClickHouseExecutorFactory`).
    """

    EXECUTOR_FACTORY_CLS: ClassVar[type[ExecutorFactoryBase]]

    def _make_query_action(self, exec_factory: ExecutorFactoryBase, sa_query: Select) -> ActionExecuteQuery:
        return ActionExecuteQuery(
            target_conn_dto=exec_factory.make_dto(),
            dba_cls=exec_factory.get_dba_class(),
            req_ctx_info=DBAdapterScopedRCI(),
            db_adapter_query=DBAdapterQuery(
                query=sa_query,
                is_dashsql_query=True,
            ),
        )

    def _check_action_serialization_deserialization(self, action: ActionExecuteQuery) -> None:
        serializer = ActionSerializer()
        serialized_action = serializer.serialize_action(action)
        deserialized_action = serializer.deserialize_action(
            serialized_action,
            allowed_dba_classes=frozenset(SUPPORTED_ADAPTER_CLS),
        )
        assert isinstance(deserialized_action, ActionExecuteQuery)
        # SA queries don't compare equal — stringify them for comparison.
        assert str(deserialized_action.db_adapter_query.query) == str(action.db_adapter_query.query)
        assert deserialized_action.db_adapter_query.is_dashsql_query

    def test_basic_query(self, db: Db) -> None:
        exec_factory = self.EXECUTOR_FACTORY_CLS(db=db)
        # The factory's adapter class may not be registered when the connector
        # is loaded standalone — register it so the round-trip can resolve it.
        register_adapter_class(exec_factory.get_dba_class())
        action = self._make_query_action(
            exec_factory=exec_factory,
            sa_query=sa.select([sa.literal(1)]),
        )
        self._check_action_serialization_deserialization(action)
