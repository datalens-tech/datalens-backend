from __future__ import annotations

import logging
from typing import ClassVar

from dl_constants.enums import CreateDSFrom

from bi_connector_bundle_ch_filtered.base.core.data_source import ClickHouseTemplatedSubselectDataSource
from bi_connector_bundle_ch_filtered_ya_cloud.smb_heatmaps.core.constants import (
    CONNECTION_TYPE_SMB_HEATMAPS, SOURCE_TYPE_CH_SMB_HEATMAPS_TABLE,
)


LOGGER = logging.getLogger(__name__)


class ClickHouseSMBHeatmapsDataSource(ClickHouseTemplatedSubselectDataSource):
    preview_enabled: ClassVar[bool] = False

    conn_type = CONNECTION_TYPE_SMB_HEATMAPS

    @classmethod
    def is_compatible_with_type(cls, source_type: CreateDSFrom) -> bool:
        return source_type in {
            SOURCE_TYPE_CH_SMB_HEATMAPS_TABLE,
        }
