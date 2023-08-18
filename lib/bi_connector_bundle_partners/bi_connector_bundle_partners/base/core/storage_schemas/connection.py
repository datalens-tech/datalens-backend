from typing import TypeVar

from bi_core.connectors.clickhouse_base.storage_schemas.connection import ConnectionClickHouseBaseDataStorageSchema
from bi_connector_bundle_partners.base.core.us_connection import PartnersCHConnectionBase


_CH_PARTNER_CONN_DATA_TV = TypeVar('_CH_PARTNER_CONN_DATA_TV', bound=PartnersCHConnectionBase.DataModel)


class PartnersCHConnectionDataStorageSchema(
        ConnectionClickHouseBaseDataStorageSchema[PartnersCHConnectionBase.DataModel],
):
    TARGET_CLS = PartnersCHConnectionBase.DataModel
