from __future__ import annotations

from bi_connector_bundle_partners.moysklad.core.constants import CONNECTION_TYPE_MOYSKLAD
from bi_connector_bundle_partners.base.core.data_source import PartnersCHDataSourceBase


class MoySkladCHDataSource(PartnersCHDataSourceBase):
    conn_type = CONNECTION_TYPE_MOYSKLAD
