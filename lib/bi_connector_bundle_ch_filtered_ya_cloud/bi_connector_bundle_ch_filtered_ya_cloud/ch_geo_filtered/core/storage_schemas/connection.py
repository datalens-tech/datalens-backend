from marshmallow import fields as ma_fields

from bi_connector_bundle_ch_filtered_ya_cloud.ch_geo_filtered.core.us_connection import ConnectionClickhouseGeoFiltered
from bi_connector_bundle_ch_filtered.base.core.storage_schemas.connection import (
    ConnectionClickhouseFilteredBaseDataStorageSchema,
)


class ConnectionClickhouseGeoFilteredDataStorageSchema(
        ConnectionClickhouseFilteredBaseDataStorageSchema[ConnectionClickhouseGeoFiltered.DataModel],
):
    TARGET_CLS = ConnectionClickhouseGeoFiltered.DataModel

    allowed_tables = ma_fields.List(ma_fields.String(), required=True, allow_none=False)
    allow_public_usage = ma_fields.Boolean(allow_none=False, required=False, dump_default=False, load_default=False)
    mp_product_id = ma_fields.String(allow_none=False, required=True)
    data_export_forbidden = ma_fields.Boolean(allow_none=False, required=False, dump_default=True, load_default=True)
