from __future__ import annotations

from bi_connector_bundle_partners.base.bi.api_schema.connection import PartnersConnectionSchemaBase
from bi_connector_bundle_partners.kontur_market.core.us_connection import KonturMarketCHConnection


class KonturMarketConnectionSchema(PartnersConnectionSchemaBase):
    TARGET_CLS = KonturMarketCHConnection
