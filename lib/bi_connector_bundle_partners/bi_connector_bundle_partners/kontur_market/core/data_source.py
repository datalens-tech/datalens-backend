from __future__ import annotations

from bi_constants.enums import ConnectionType

from bi_connector_bundle_partners.base.core.data_source import PartnersCHDataSourceBase


class KonturMarketCHDataSource(PartnersCHDataSourceBase):
    conn_type = ConnectionType.kontur_market
