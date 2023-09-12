from marshmallow import fields as ma_fields

from bi_connector_bundle_ch_filtered.base.core.storage_schemas.connection import (
    ConnectionClickhouseFilteredBaseDataStorageSchema,
)
from bi_connector_bundle_ch_filtered_ya_cloud.base.core.us_connection import ConnectionCHFilteredSubselectByPuidBase


class ConnectionCHFilteredSubselectByPuidDataStorageSchema(
        ConnectionClickhouseFilteredBaseDataStorageSchema[ConnectionCHFilteredSubselectByPuidBase.DataModel],
):
    TARGET_CLS = ConnectionCHFilteredSubselectByPuidBase.DataModel

    token = ma_fields.String(allow_none=False, required=True)
