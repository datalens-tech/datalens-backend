from bi_core.connections_security.base import ConnSecuritySettings
from bi_api_lib_ya.connections_security.base import MDBConnectionSafetyChecker
from bi_connector_clickhouse.core.clickhouse.connector import (
    ClickHouseCoreConnectionDefinition,
    ClickHouseTableCoreSourceDefinition,
    ClickHouseSubselectCoreSourceDefinition,
    ClickHouseCoreConnector,
)
from bi_connector_clickhouse.core.clickhouse_base.dto import ClickHouseConnDTO
from bi_connector_clickhouse_mdb.core.data_source import ClickHouseMDBDataSource, ClickHouseMDBSubselectDataSource
from bi_connector_clickhouse_mdb.core.settings import ClickHouseMDBSettingDefinition
from bi_connector_clickhouse_mdb.core.storage_schemas import ConnectionClickhouseMDBDataStorageSchema
from bi_connector_clickhouse_mdb.core.us_connection import ConnectionClickhouseMDB


class ClickHouseMDBCoreConnectionDefinition(ClickHouseCoreConnectionDefinition):
    connection_cls = ConnectionClickhouseMDB
    us_storage_schema_cls = ConnectionClickhouseMDBDataStorageSchema
    settings_definition = ClickHouseMDBSettingDefinition


class ClickHouseMDBTableCoreSourceDefinition(ClickHouseTableCoreSourceDefinition):
    source_cls = ClickHouseMDBDataSource


class ClickHouseMDBSubselectCoreSourceDefinition(ClickHouseSubselectCoreSourceDefinition):
    source_cls = ClickHouseMDBSubselectDataSource


class ClickHouseMDBCoreConnector(ClickHouseCoreConnector):
    connection_definitions = (
        ClickHouseMDBCoreConnectionDefinition,
    )
    source_definitions = (
        ClickHouseMDBTableCoreSourceDefinition,
        ClickHouseMDBSubselectCoreSourceDefinition,
    )
    conn_security = frozenset({
        ConnSecuritySettings(MDBConnectionSafetyChecker, frozenset({ClickHouseConnDTO})),
    })
