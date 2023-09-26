from __future__ import annotations

from bi_connector_bundle_partners.base.api.api_schema.connection import PartnersConnectionSchemaBase
from bi_connector_bundle_partners.moysklad.core.us_connection import MoySkladCHConnection


class MoySkladConnectionSchema(PartnersConnectionSchemaBase):
    TARGET_CLS = MoySkladCHConnection
