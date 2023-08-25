from __future__ import annotations

from bi_core.connectors.base.connector import (
    CoreConnectionDefinition, CoreSourceDefinition,
)
from bi_core.connectors.clickhouse_base.connector import ClickHouseCoreConnectorBase
from bi_core.connectors.clickhouse_base.connection_executors import (
    ClickHouseSyncAdapterConnExecutor, ClickHouseAsyncAdapterConnExecutor,
)
from bi_core.connectors.clickhouse_base.type_transformer import ClickHouseTypeTransformer
from bi_core.us_manager.storage_schemas.data_source_spec_base import SQLDataSourceSpecStorageSchema
from bi_core.data_source_spec.sql import StandardSQLDataSourceSpec

from bi_connector_bundle_ch_filtered_ya_cloud.schoolbook.core.constants import (
    CONNECTION_TYPE_SCHOOLBOOK_JOURNAL, SOURCE_TYPE_CH_SCHOOLBOOK_TABLE,
)
from bi_connector_bundle_ch_filtered_ya_cloud.base.core.lifecycle import (
    CHFilteredSubselectByPuidBaseConnectionLifecycleManager,
)
from bi_connector_bundle_ch_filtered_ya_cloud.base.core.storage_schemas.connection import (
    ConnectionCHFilteredSubselectByPuidDataStorageSchema,
)
from bi_connector_bundle_ch_filtered_ya_cloud.schoolbook.core.data_source import ClickHouseSchoolbookDataSource
from bi_connector_bundle_ch_filtered_ya_cloud.schoolbook.core.settings import CHSchoolbookSettingDefinition
from bi_connector_bundle_ch_filtered_ya_cloud.schoolbook.core.us_connection import ConnectionClickhouseSchoolbook


class CHSchoolbookCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_SCHOOLBOOK_JOURNAL
    connection_cls = ConnectionClickhouseSchoolbook
    us_storage_schema_cls = ConnectionCHFilteredSubselectByPuidDataStorageSchema
    type_transformer_cls = ClickHouseTypeTransformer
    sync_conn_executor_cls = ClickHouseSyncAdapterConnExecutor
    async_conn_executor_cls = ClickHouseAsyncAdapterConnExecutor
    lifecycle_manager_cls = CHFilteredSubselectByPuidBaseConnectionLifecycleManager
    dialect_string = 'bi_clickhouse'
    settings_definition = CHSchoolbookSettingDefinition


class CHSchoolbookCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_CH_SCHOOLBOOK_TABLE
    source_cls = ClickHouseSchoolbookDataSource
    source_spec_cls = StandardSQLDataSourceSpec
    us_storage_schema_cls = SQLDataSourceSpecStorageSchema


class CHSchoolbookCoreConnector(ClickHouseCoreConnectorBase):
    connection_definitions = (
        CHSchoolbookCoreConnectionDefinition,
    )
    source_definitions = (
        CHSchoolbookCoreSourceDefinition,
    )
