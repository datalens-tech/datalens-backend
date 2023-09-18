from __future__ import annotations

import attr

from bi_connector_bundle_ch_filtered.base.core.us_connection import ConnectionClickhouseFilteredBase
from bi_connector_bundle_ch_filtered_ya_cloud.ch_geo_filtered.core.constants import SOURCE_TYPE_CH_GEO_FILTERED_TABLE


class ConnectionClickhouseGeoFiltered(ConnectionClickhouseFilteredBase):
    source_type = SOURCE_TYPE_CH_GEO_FILTERED_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_CH_GEO_FILTERED_TABLE,))
    allow_cache = True

    @attr.s(kw_only=True)
    class DataModel(ConnectionClickhouseFilteredBase.DataModel):
        allowed_tables: list[str] = attr.ib(factory=list)
        allow_public_usage: bool = attr.ib(default=False)
        mp_product_id: str = attr.ib()
        data_export_forbidden: bool = attr.ib(default=True)

    @property
    def allowed_tables(self) -> list[str]:
        return self.data.allowed_tables

    @property
    def allow_public_usage(self) -> bool:
        return self.data.allow_public_usage
