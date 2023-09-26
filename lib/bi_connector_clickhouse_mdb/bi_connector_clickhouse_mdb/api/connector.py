from dl_connector_clickhouse.api.connector import (
    ClickHouseApiConnectionDefinition,
    ClickHouseApiConnector,
)

from bi_connector_clickhouse_mdb.api.api_schema.connection import ClickHouseMDBConnectionSchema
from bi_connector_clickhouse_mdb.api.connection_form.form_config import ClickHouseMDBConnectionFormFactory
from bi_connector_clickhouse_mdb.core.connector import ClickHouseMDBCoreConnector


class ClickHouseMDBApiConnectionDefinition(ClickHouseApiConnectionDefinition):
    api_generic_schema_cls = ClickHouseMDBConnectionSchema
    form_factory_cls = ClickHouseMDBConnectionFormFactory


class ClickHouseMDBApiConnector(ClickHouseApiConnector):
    core_connector_cls = ClickHouseMDBCoreConnector
    connection_definitions = (ClickHouseMDBApiConnectionDefinition,)
