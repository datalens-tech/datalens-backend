from __future__ import annotations

from bi_connector_bundle_partners.base.api.api_schema.connection import PartnersConnectionSchemaBase
from bi_connector_bundle_partners.equeo.core.us_connection import EqueoCHConnection


class EqueoConnectionSchema(PartnersConnectionSchemaBase):
    TARGET_CLS = EqueoCHConnection
