from __future__ import annotations

from dl_core.connections_security.base import NonUserInputConnectionSafetyChecker, ConnSecuritySettings
from dl_core.data_source_spec.sql import StandardSQLDataSourceSpec
from dl_core.us_manager.storage_schemas.data_source_spec_base import SQLDataSourceSpecStorageSchema
from dl_core.connectors.base.connector import (
    CoreConnectionDefinition, CoreConnector, CoreSourceDefinition,
)

from bi_connector_metrica.core.constants import (
    BACKEND_TYPE_METRICA_API, BACKEND_TYPE_APPMETRICA_API,
    CONNECTION_TYPE_METRICA_API, CONNECTION_TYPE_APPMETRICA_API,
    SOURCE_TYPE_METRICA_API, SOURCE_TYPE_APPMETRICA_API,
)
from bi_connector_metrica.core.adapters_metrica_x import (
    MetricaAPIDefaultAdapter,
    AppMetricaAPIDefaultAdapter,
)
from bi_connector_metrica.core.connection_executors import (
    MetricaAPIConnExecutor, AppMetricaAPIConnExecutor
)
from bi_connector_metrica.core.data_source import MetrikaApiDataSource, AppMetrikaApiDataSource
from bi_connector_metrica.core.lifecycle import MetricaConnectionLifecycleManager
from bi_connector_metrica.core.storage_schemas.connection import (
    ConnectionMetrikaApiDataStorageSchema,
    ConnectionAppMetricaApiDataStorageSchema,
)
from bi_connector_metrica.core.type_transformer import MetrikaApiTypeTransformer
from bi_connector_metrica.core.us_connection import MetrikaApiConnection, AppMetricaApiConnection
from bi_connector_metrica.core.dto import MetricaAPIConnDTO, AppMetricaAPIConnDTO
from bi_connector_metrica.core.settings import MetricaSettingDefinition, AppMetricaSettingDefinition
from bi_connector_metrica.core.data_source_migration import (
    AppMetricaApiDataSourceMigrator,
    MetricaApiDataSourceMigrator,
)


class MetricaApiCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_METRICA_API
    connection_cls = MetrikaApiConnection
    us_storage_schema_cls = ConnectionMetrikaApiDataStorageSchema
    type_transformer_cls = MetrikaApiTypeTransformer
    sync_conn_executor_cls = MetricaAPIConnExecutor
    async_conn_executor_cls = MetricaAPIConnExecutor
    lifecycle_manager_cls = MetricaConnectionLifecycleManager
    dialect_string = 'metrika_api'
    settings_definition = MetricaSettingDefinition
    data_source_migrator_cls = MetricaApiDataSourceMigrator


class MetricaApiCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_METRICA_API
    source_cls = MetrikaApiDataSource
    source_spec_cls = StandardSQLDataSourceSpec
    us_storage_schema_cls = SQLDataSourceSpecStorageSchema


class MetricaApiCoreConnector(CoreConnector):
    backend_type = BACKEND_TYPE_METRICA_API
    connection_definitions = (
        MetricaApiCoreConnectionDefinition,
    )
    source_definitions = (
        MetricaApiCoreSourceDefinition,
    )
    rqe_adapter_classes = frozenset({MetricaAPIDefaultAdapter})
    conn_security = frozenset({
        ConnSecuritySettings(NonUserInputConnectionSafetyChecker, frozenset({MetricaAPIConnDTO})),
    })


class AppMetricaApiCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_APPMETRICA_API
    connection_cls = AppMetricaApiConnection
    us_storage_schema_cls = ConnectionAppMetricaApiDataStorageSchema
    type_transformer_cls = MetrikaApiTypeTransformer
    sync_conn_executor_cls = AppMetricaAPIConnExecutor
    async_conn_executor_cls = AppMetricaAPIConnExecutor
    lifecycle_manager_cls = MetricaConnectionLifecycleManager
    dialect_string = 'appmetrica_api'
    settings_definition = AppMetricaSettingDefinition
    data_source_migrator_cls = AppMetricaApiDataSourceMigrator


class AppMetricaApiCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_APPMETRICA_API
    source_cls = AppMetrikaApiDataSource
    source_spec_cls = StandardSQLDataSourceSpec
    us_storage_schema_cls = SQLDataSourceSpecStorageSchema


class AppMetricaApiCoreConnector(CoreConnector):
    backend_type = BACKEND_TYPE_APPMETRICA_API
    connection_definitions = (
        AppMetricaApiCoreConnectionDefinition,
    )
    source_definitions = (
        AppMetricaApiCoreSourceDefinition,
    )
    rqe_adapter_classes = frozenset({AppMetricaAPIDefaultAdapter})
    conn_security = frozenset({
        ConnSecuritySettings(NonUserInputConnectionSafetyChecker, frozenset({AppMetricaAPIConnDTO})),
    })
