from __future__ import annotations

from bi_connector_bundle_partners.base.core.data_source import PartnersCHDataSourceBase

from bi_connector_bundle_partners.equeo.core.constants import CONNECTION_TYPE_EQUEO


class EqueoCHDataSource(PartnersCHDataSourceBase):
    conn_type = CONNECTION_TYPE_EQUEO
