from typing import Generic, TypeVar

from bi_core.connectors.clickhouse_base.storage_schemas.connection import ConnectionClickHouseBaseDataStorageSchema

from bi_connector_bundle_ch_filtered.base.core.us_connection import (
    ConnectionCHFilteredHardcodedDataBase, ConnectionClickhouseFilteredBase,
)


_CH_FILTERED_CONN_DATA_TV = TypeVar('_CH_FILTERED_CONN_DATA_TV', bound=ConnectionClickhouseFilteredBase.DataModel)


class ConnectionClickhouseFilteredBaseDataStorageSchema(
        ConnectionClickHouseBaseDataStorageSchema[_CH_FILTERED_CONN_DATA_TV], Generic[_CH_FILTERED_CONN_DATA_TV],
):
    pass


class ConnectionCHFilteredHardcodedDataBaseDataStorageSchema(
        ConnectionClickhouseFilteredBaseDataStorageSchema[ConnectionCHFilteredHardcodedDataBase.DataModel],
):
    TARGET_CLS = ConnectionCHFilteredHardcodedDataBase.DataModel
