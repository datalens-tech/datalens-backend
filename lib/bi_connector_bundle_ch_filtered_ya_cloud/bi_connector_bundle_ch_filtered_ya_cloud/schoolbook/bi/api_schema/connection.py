from __future__ import annotations

from bi_connector_bundle_ch_filtered_ya_cloud.schoolbook.core.us_connection import ConnectionClickhouseSchoolbook

from bi_api_connector.api_schema.connection_base import ConnectionSchema, ConnectionMetaMixin
from bi_api_connector.api_schema.connection_base_fields import secret_string_field


class CHSchoolbookConnectionSchema(ConnectionMetaMixin, ConnectionSchema):
    TARGET_CLS = ConnectionClickhouseSchoolbook

    token = secret_string_field(attribute='data.token')
