from __future__ import annotations

from typing import ClassVar, TYPE_CHECKING

from bi_connector_bundle_ch_filtered_ya_cloud.smb_heatmaps.core.constants import (
    SOURCE_TYPE_CH_SMB_HEATMAPS_TABLE,
)
from bi_connector_bundle_ch_filtered_ya_cloud.base.core.us_connection import ConnectionCHFilteredSubselectByPuidBase

if TYPE_CHECKING:
    from bi_configs.connectors_settings import SMBHeatmapsConnectorSettings


class ConnectionClickhouseSMBHeatmaps(ConnectionCHFilteredSubselectByPuidBase):
    source_type = SOURCE_TYPE_CH_SMB_HEATMAPS_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_CH_SMB_HEATMAPS_TABLE,))
    is_always_internal_source: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True

    @property
    def _connector_settings(self) -> SMBHeatmapsConnectorSettings:
        settings = self._all_connectors_settings.SMB_HEATMAPS
        assert settings is not None
        return settings
