from __future__ import annotations

from bi_connector_bundle_partners.base.core.us_connection import PartnersCHConnectionBase
from bi_connector_bundle_partners.equeo.core.constants import SOURCE_TYPE_EQUEO_CH_TABLE


class EqueoCHConnection(PartnersCHConnectionBase):
    source_type = SOURCE_TYPE_EQUEO_CH_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_EQUEO_CH_TABLE,))
