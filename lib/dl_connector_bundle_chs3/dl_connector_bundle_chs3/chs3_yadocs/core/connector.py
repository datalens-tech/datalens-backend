from dl_connector_bundle_chs3.chs3_base.core.connector import (
    BaseFileS3CoreConnectionDefinition,
    BaseFileS3CoreConnector,
    BaseFileS3TableCoreSourceDefinition,
    CHS3CoreBackendDefinition,
)
from dl_connector_bundle_chs3.chs3_base.core.notifications import (
    DataUpdateFailureNotification,
    StaleDataNotification,
)
from dl_connector_bundle_chs3.chs3_yadocs.core.adapter import AsyncYaDocsFileS3Adapter
from dl_connector_bundle_chs3.chs3_yadocs.core.connection_executors import YaDocsFileS3AsyncAdapterConnExecutor
from dl_connector_bundle_chs3.chs3_yadocs.core.constants import (
    BACKEND_TYPE_YADOCS,
    CONNECTION_TYPE_YADOCS,
    SOURCE_TYPE_YADOCS,
)
from dl_connector_bundle_chs3.chs3_yadocs.core.data_source import YaDocsFileS3DataSource
from dl_connector_bundle_chs3.chs3_yadocs.core.data_source_spec import YaDocsFileS3DataSourceSpec
from dl_connector_bundle_chs3.chs3_yadocs.core.lifecycle import YaDocsFileS3ConnectionLifecycleManager
from dl_connector_bundle_chs3.chs3_yadocs.core.sa_types import SQLALCHEMY_YADOCS_TYPES
from dl_connector_bundle_chs3.chs3_yadocs.core.settings import YaDocsFileS3SettingDefinition
from dl_connector_bundle_chs3.chs3_yadocs.core.storage_schemas.connection import YaDocsFileConnectionDataStorageSchema
from dl_connector_bundle_chs3.chs3_yadocs.core.storage_schemas.data_source_spec import (
    YaDocsFileS3DataSourceSpecStorageSchema,
)
from dl_connector_bundle_chs3.chs3_yadocs.core.us_connection import YaDocsFileS3Connection
from dl_connector_clickhouse.core.clickhouse_base.adapters import ClickHouseAdapter


class YaDocsFileS3CoreConnectionDefinition(BaseFileS3CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_YADOCS
    connection_cls = YaDocsFileS3Connection
    us_storage_schema_cls = YaDocsFileConnectionDataStorageSchema
    sync_conn_executor_cls = YaDocsFileS3AsyncAdapterConnExecutor
    async_conn_executor_cls = YaDocsFileS3AsyncAdapterConnExecutor
    lifecycle_manager_cls = YaDocsFileS3ConnectionLifecycleManager
    dialect_string = "bi_clickhouse"
    settings_definition = YaDocsFileS3SettingDefinition


class YaDocsFileS3TableCoreSourceDefinition(BaseFileS3TableCoreSourceDefinition):
    source_type = SOURCE_TYPE_YADOCS
    source_cls = YaDocsFileS3DataSource
    source_spec_cls = YaDocsFileS3DataSourceSpec
    us_storage_schema_cls = YaDocsFileS3DataSourceSpecStorageSchema


class YaDocsFileS3CoreBackendDefinition(CHS3CoreBackendDefinition):
    backend_type = BACKEND_TYPE_YADOCS


class YaDocsFileS3CoreConnector(BaseFileS3CoreConnector):
    backend_definition = YaDocsFileS3CoreBackendDefinition
    connection_definitions = (YaDocsFileS3CoreConnectionDefinition,)
    source_definitions = (YaDocsFileS3TableCoreSourceDefinition,)
    rqe_adapter_classes = frozenset({ClickHouseAdapter, AsyncYaDocsFileS3Adapter})
    notification_classes = (
        StaleDataNotification,
        DataUpdateFailureNotification,
    )
    sa_types = SQLALCHEMY_YADOCS_TYPES
