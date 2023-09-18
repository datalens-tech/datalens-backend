from __future__ import annotations

from typing import ClassVar

from bi_connector_bundle_ch_filtered_ya_cloud.base.core.us_connection import ConnectionCHFilteredSubselectByPuidBase
from bi_connector_bundle_ch_filtered_ya_cloud.smb_heatmaps.core.constants import SOURCE_TYPE_CH_SMB_HEATMAPS_TABLE
from bi_connector_bundle_ch_filtered_ya_cloud.smb_heatmaps.core.settings import SMBHeatmapsConnectorSettings


class ConnectionClickhouseSMBHeatmaps(ConnectionCHFilteredSubselectByPuidBase[SMBHeatmapsConnectorSettings]):
    source_type = SOURCE_TYPE_CH_SMB_HEATMAPS_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_CH_SMB_HEATMAPS_TABLE,))
    is_always_internal_source: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True
    settings_type = SMBHeatmapsConnectorSettings
