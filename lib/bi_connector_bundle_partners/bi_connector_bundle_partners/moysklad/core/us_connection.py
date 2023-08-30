from __future__ import annotations

from bi_connector_bundle_partners.base.core.us_connection import PartnersCHConnectionBase
from bi_connector_bundle_partners.moysklad.core.constants import SOURCE_TYPE_MOYSKLAD_CH_TABLE


class MoySkladCHConnection(PartnersCHConnectionBase):
    source_type = SOURCE_TYPE_MOYSKLAD_CH_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_MOYSKLAD_CH_TABLE,))
