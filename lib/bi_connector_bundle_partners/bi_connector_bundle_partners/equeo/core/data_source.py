from __future__ import annotations

from bi_constants.enums import ConnectionType

from bi_connector_bundle_partners.base.core.data_source import PartnersCHDataSourceBase


class EqueoCHDataSource(PartnersCHDataSourceBase):
    conn_type = ConnectionType.equeo
