from clickhouse_sqlalchemy.orm.query import Query as CHQuery

from bi_core.connectors.base.connector import CoreConnector

from bi_connector_clickhouse.core.clickhouse_base.adapters import (
    AsyncClickHouseAdapter,
    ClickHouseAdapter,
)
from bi_connector_clickhouse.core.clickhouse_base.constants import BACKEND_TYPE_CLICKHOUSE
from bi_connector_clickhouse.core.clickhouse_base.query_compiler import ClickHouseQueryCompiler
from bi_connector_clickhouse.core.clickhouse_base.sa_types import SQLALCHEMY_CLICKHOUSE_TYPES


class ClickHouseCoreConnectorBase(CoreConnector):
    backend_type = BACKEND_TYPE_CLICKHOUSE
    compiler_cls = ClickHouseQueryCompiler
    query_cls = CHQuery
    rqe_adapter_classes = frozenset({ClickHouseAdapter, AsyncClickHouseAdapter})
    sa_types = SQLALCHEMY_CLICKHOUSE_TYPES
