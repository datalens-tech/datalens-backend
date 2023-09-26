from __future__ import annotations

from dl_api_connector.api_schema.connection_base import (
    ConnectionMetaMixin,
    ConnectionSchema,
)
from dl_api_connector.api_schema.connection_base_fields import secret_string_field

from bi_connector_bundle_ch_filtered_ya_cloud.smb_heatmaps.core.us_connection import ConnectionClickhouseSMBHeatmaps


class CHSMBHeatmapsConnectionSchema(ConnectionMetaMixin, ConnectionSchema):
    TARGET_CLS = ConnectionClickhouseSMBHeatmaps

    token = secret_string_field(attribute="data.token")
