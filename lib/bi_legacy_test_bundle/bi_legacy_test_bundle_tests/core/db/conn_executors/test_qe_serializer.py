import sqlalchemy as sa
from sqlalchemy.sql.selectable import Select

from bi_core.connection_executors.qe_serializer.serializer import ActionSerializer
from bi_core.connection_executors.qe_serializer.dba_actions import ActionExecuteQuery
from bi_core.connection_executors.models.db_adapter_data import DBAdapterQuery
from bi_core.connection_executors.remote_query_executor.commons import SUPPORTED_ADAPTER_CLS
from bi_api_commons.base_models import RequestContextInfo
from bi_core_testing.executors import ExecutorFactoryBase

from bi_connector_clickhouse.core.testing.exec_factory import ClickHouseExecutorFactory


def _check_action_serialization_deserialization(action: ActionExecuteQuery) -> None:
    serializer = ActionSerializer()
    serialized_action = serializer.serialize_action(action)
    deserialized_action = serializer.deserialize_action(serialized_action, allowed_dba_classes=SUPPORTED_ADAPTER_CLS)
    assert isinstance(deserialized_action, ActionExecuteQuery)
    # Direct comparison of action objects doesn't work because SA queries never equal one another.
    # So just compare the stringified queries.
    assert str(deserialized_action.db_adapter_query.query) == str(action.db_adapter_query.query)
    assert deserialized_action.db_adapter_query.is_dashsql_query


def _make_query_action(exec_factory: ExecutorFactoryBase, sa_query: Select) -> ActionExecuteQuery:
    action = ActionExecuteQuery(
        target_conn_dto=exec_factory.make_dto(),
        dba_cls=exec_factory.get_dba_class(),
        req_ctx_info=RequestContextInfo(),
        db_adapter_query=DBAdapterQuery(
            query=sa_query,
            is_dashsql_query=True,
        ),
    )
    return action


def test_basic_query(db):
    action = _make_query_action(
        exec_factory=ClickHouseExecutorFactory(db=db),
        sa_query=sa.select([sa.literal(1)]),
    )
    _check_action_serialization_deserialization(action)
