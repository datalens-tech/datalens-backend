from __future__ import annotations

from bi_core.connectors.base.connector import (
    CoreConnectionDefinition, CoreSourceDefinition,
)
from bi_connector_clickhouse.core.clickhouse_base.connector import ClickHouseCoreConnectorBase
from bi_connector_clickhouse.core.clickhouse_base.connection_executors import (
    ClickHouseSyncAdapterConnExecutor, ClickHouseAsyncAdapterConnExecutor,
)
from bi_connector_clickhouse.core.clickhouse_base.type_transformer import ClickHouseTypeTransformer

from bi_connector_bundle_ch_filtered_ya_cloud.ch_geo_filtered.core.constants import (
    CONNECTION_TYPE_CH_GEO_FILTERED, SOURCE_TYPE_CH_GEO_FILTERED_TABLE,
)
from bi_connector_bundle_ch_filtered_ya_cloud.ch_geo_filtered.core.data_source import ClickHouseGeoFilteredDataSource
from bi_connector_bundle_ch_filtered_ya_cloud.ch_geo_filtered.core.us_connection import ConnectionClickhouseGeoFiltered
from bi_connector_bundle_ch_filtered_ya_cloud.ch_geo_filtered.core.storage_schemas.connection import (
    ConnectionClickhouseGeoFilteredDataStorageSchema
)
from bi_core.us_manager.storage_schemas.data_source_spec_base import SQLDataSourceSpecStorageSchema
from bi_core.data_source_spec.sql import StandardSQLDataSourceSpec


class ClickhouseGeoFilteredCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_CH_GEO_FILTERED
    connection_cls = ConnectionClickhouseGeoFiltered
    us_storage_schema_cls = ConnectionClickhouseGeoFilteredDataStorageSchema
    type_transformer_cls = ClickHouseTypeTransformer
    sync_conn_executor_cls = ClickHouseSyncAdapterConnExecutor
    async_conn_executor_cls = ClickHouseAsyncAdapterConnExecutor
    dialect_string = 'bi_clickhouse'


class ClickhouseGeoFilteredTableCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_CH_GEO_FILTERED_TABLE
    source_cls = ClickHouseGeoFilteredDataSource
    source_spec_cls = StandardSQLDataSourceSpec
    us_storage_schema_cls = SQLDataSourceSpecStorageSchema


class ClickhouseGeoFilteredCoreConnector(ClickHouseCoreConnectorBase):
    connection_definitions = (
        ClickhouseGeoFilteredCoreConnectionDefinition,
    )
    source_definitions = (
        ClickhouseGeoFilteredTableCoreSourceDefinition,
    )
