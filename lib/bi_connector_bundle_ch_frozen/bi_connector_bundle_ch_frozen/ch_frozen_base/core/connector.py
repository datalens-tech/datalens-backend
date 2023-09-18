from __future__ import annotations

from dl_connector_clickhouse.core.clickhouse_base.connection_executors import (
    ClickHouseAsyncAdapterConnExecutor,
    ClickHouseSyncAdapterConnExecutor,
)
from dl_connector_clickhouse.core.clickhouse_base.connector import ClickHouseCoreConnectorBase
from dl_connector_clickhouse.core.clickhouse_base.type_transformer import ClickHouseTypeTransformer
from dl_core.connectors.base.connector import (
    CoreConnectionDefinition,
    CoreSourceDefinition,
)
from dl_core.data_source_spec.sql import (
    StandardSQLDataSourceSpec,
    SubselectDataSourceSpec,
)
from dl_core.us_manager.storage_schemas.data_source_spec_base import (
    SQLDataSourceSpecStorageSchema,
    SubselectDataSourceSpecStorageSchema,
)

from bi_connector_bundle_ch_filtered.base.core.storage_schemas.connection import (
    ConnectionCHFilteredHardcodedDataBaseDataStorageSchema,
)
from bi_connector_bundle_ch_frozen.ch_frozen_base.core.constants import (
    SOURCE_TYPE_CH_FROZEN_SOURCE,
    SOURCE_TYPE_CH_FROZEN_SUBSELECT,
)
from bi_connector_bundle_ch_frozen.ch_frozen_base.core.data_source import ClickHouseFrozenSubselectDataSourceBase
from bi_connector_bundle_ch_frozen.ch_frozen_base.core.us_connection import ConnectionClickhouseFrozenBase


class CHFrozenBaseCoreConnectionDefinition(CoreConnectionDefinition):
    connection_cls = ConnectionClickhouseFrozenBase
    us_storage_schema_cls = ConnectionCHFilteredHardcodedDataBaseDataStorageSchema
    type_transformer_cls = ClickHouseTypeTransformer
    sync_conn_executor_cls = ClickHouseSyncAdapterConnExecutor
    async_conn_executor_cls = ClickHouseAsyncAdapterConnExecutor
    dialect_string = "bi_clickhouse"


class CHFrozenBaseCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_CH_FROZEN_SOURCE
    source_spec_cls = StandardSQLDataSourceSpec
    us_storage_schema_cls = SQLDataSourceSpecStorageSchema


class CHFrozenBaseCoreSubselectSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_CH_FROZEN_SUBSELECT
    source_cls = ClickHouseFrozenSubselectDataSourceBase
    source_spec_cls = SubselectDataSourceSpec
    us_storage_schema_cls = SubselectDataSourceSpecStorageSchema


class CHFrozenCoreConnector(ClickHouseCoreConnectorBase):
    connection_definitions = (CHFrozenBaseCoreConnectionDefinition,)
    source_definitions = (CHFrozenBaseCoreSourceDefinition,)
