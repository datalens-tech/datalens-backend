from __future__ import annotations

import logging
from typing import Iterable, TYPE_CHECKING

from bi_constants.enums import CreateDSFrom

from bi_core.base_models import SourceFilterSpec

from bi_connector_bundle_ch_filtered.base.core.data_source import ClickHouseFilteredDataSourceBase
from bi_connector_bundle_ch_filtered_ya_cloud.ch_geo_filtered.core.constants import (
    CONNECTION_TYPE_CH_GEO_FILTERED, SOURCE_TYPE_CH_GEO_FILTERED_TABLE,
)

if TYPE_CHECKING:
    from bi_core.services_registry.top_level import ServicesRegistry


LOGGER = logging.getLogger(__name__)


class ClickHouseGeoFilteredDataSource(ClickHouseFilteredDataSourceBase):
    """
    Geo layers
    """

    conn_type = CONNECTION_TYPE_CH_GEO_FILTERED

    def get_filters(self, service_registry: ServicesRegistry) -> Iterable[SourceFilterSpec]:
        # TODO: cleanup GeoFiltered conn
        return super().get_filters(service_registry)

    @classmethod
    def is_compatible_with_type(cls, source_type: CreateDSFrom) -> bool:
        return source_type in {
            SOURCE_TYPE_CH_GEO_FILTERED_TABLE,
        }
