from __future__ import annotations

from bi_connector_bundle_partners.base.core.data_source import PartnersCHDataSourceBase
from bi_connector_bundle_partners.kontur_market.core.constants import CONNECTION_TYPE_KONTUR_MARKET


class KonturMarketCHDataSource(PartnersCHDataSourceBase):
    conn_type = CONNECTION_TYPE_KONTUR_MARKET
