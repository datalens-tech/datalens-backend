from dl_connector_clickhouse.bi.connector import (
    ClickHouseBiApiConnectionDefinition,
    ClickHouseBiApiConnector,
)

from bi_connector_clickhouse_mdb.bi.api_schema.connection import ClickHouseMDBConnectionSchema
from bi_connector_clickhouse_mdb.bi.connection_form.form_config import ClickHouseMDBConnectionFormFactory
from bi_connector_clickhouse_mdb.core.connector import ClickHouseMDBCoreConnector


class ClickHouseMDBBiApiConnectionDefinition(ClickHouseBiApiConnectionDefinition):
    api_generic_schema_cls = ClickHouseMDBConnectionSchema
    form_factory_cls = ClickHouseMDBConnectionFormFactory


class ClickHouseMDBBiApiConnector(ClickHouseBiApiConnector):
    core_connector_cls = ClickHouseMDBCoreConnector
    connection_definitions = (ClickHouseMDBBiApiConnectionDefinition,)
