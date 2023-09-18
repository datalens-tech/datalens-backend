from __future__ import annotations

from marshmallow import fields as ma_fields

from bi_connector_bundle_ch_filtered_ya_cloud.ch_geo_filtered.core.us_connection import ConnectionClickhouseGeoFiltered

from dl_api_connector.api_schema.connection_base import ConnectionMetaMixin, ConnectionSchema


class CHGeoFilteredConnectionSchema(ConnectionMetaMixin, ConnectionSchema):
    TARGET_CLS = ConnectionClickhouseGeoFiltered

    mp_product_id = ma_fields.String(attribute='data.mp_product_id')
