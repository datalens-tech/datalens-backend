from __future__ import annotations

from bi_connector_bundle_partners.base.core.us_connection import PartnersCHConnectionBase
from bi_connector_bundle_partners.kontur_market.core.constants import SOURCE_TYPE_KONTUR_MARKET_CH_TABLE


class KonturMarketCHConnection(PartnersCHConnectionBase):
    source_type = SOURCE_TYPE_KONTUR_MARKET_CH_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_KONTUR_MARKET_CH_TABLE,))
