from dl_core.connectors.base.connector import (
    CoreBackendDefinition,
    CoreConnectionDefinition,
    CoreConnector,
)
from dl_core.connectors.sql_base.connector import (
    SQLSubselectCoreSourceDefinitionBase,
    SQLTableCoreSourceDefinitionBase,
)

from dl_connector_starrocks.core.adapters_starrocks import StarRocksAdapter
from dl_connector_starrocks.core.async_adapters_starrocks import AsyncStarRocksAdapter
from dl_connector_starrocks.core.connection_executors import (
    AsyncStarRocksConnExecutor,
    StarRocksConnExecutor,
)
from dl_connector_starrocks.core.constants import (
    BACKEND_TYPE_STARROCKS,
    CONNECTION_TYPE_STARROCKS,
    SOURCE_TYPE_STARROCKS_SUBSELECT,
    SOURCE_TYPE_STARROCKS_TABLE,
)
from dl_connector_starrocks.core.data_source import (
    StarRocksDataSource,
    StarRocksSubselectDataSource,
)
from dl_connector_starrocks.core.data_source_migration import StarRocksDataSourceMigrator
from dl_connector_starrocks.core.query_compiler import StarRocksQueryCompiler
from dl_connector_starrocks.core.sa_types import SQLALCHEMY_STARROCKS_TYPES
from dl_connector_starrocks.core.settings import StarRocksSettingDefinition
from dl_connector_starrocks.core.storage_schemas.connection import ConnectionStarRocksDataStorageSchema
from dl_connector_starrocks.core.type_transformer import StarRocksTypeTransformer
from dl_connector_starrocks.core.us_connection import ConnectionStarRocks


class StarRocksCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_STARROCKS
    connection_cls = ConnectionStarRocks
    us_storage_schema_cls = ConnectionStarRocksDataStorageSchema
    type_transformer_cls = StarRocksTypeTransformer
    sync_conn_executor_cls = StarRocksConnExecutor
    async_conn_executor_cls = AsyncStarRocksConnExecutor
    dialect_string = "dl_mysql"
    data_source_migrator_cls = StarRocksDataSourceMigrator
    settings_definition = StarRocksSettingDefinition
    allow_export = True


class StarRocksTableCoreSourceDefinition(SQLTableCoreSourceDefinitionBase):
    source_type = SOURCE_TYPE_STARROCKS_TABLE
    source_cls = StarRocksDataSource


class StarRocksSubselectCoreSourceDefinition(SQLSubselectCoreSourceDefinitionBase):
    source_type = SOURCE_TYPE_STARROCKS_SUBSELECT
    source_cls = StarRocksSubselectDataSource


class StarRocksCoreBackendDefinition(CoreBackendDefinition):
    backend_type = BACKEND_TYPE_STARROCKS
    compiler_cls = StarRocksQueryCompiler


class StarRocksCoreConnector(CoreConnector):
    backend_definition = StarRocksCoreBackendDefinition
    connection_definitions = (StarRocksCoreConnectionDefinition,)
    source_definitions = (
        StarRocksTableCoreSourceDefinition,
        StarRocksSubselectCoreSourceDefinition,
    )
    rqe_adapter_classes = frozenset({StarRocksAdapter, AsyncStarRocksAdapter})  # type: ignore  # 2024-01-24 # TODO: fix abstract adapter issue
    sa_types = SQLALCHEMY_STARROCKS_TYPES
