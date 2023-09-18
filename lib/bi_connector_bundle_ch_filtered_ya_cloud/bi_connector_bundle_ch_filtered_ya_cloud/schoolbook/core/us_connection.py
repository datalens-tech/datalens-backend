from __future__ import annotations

from typing import ClassVar

from bi_connector_bundle_ch_filtered_ya_cloud.base.core.us_connection import ConnectionCHFilteredSubselectByPuidBase
from bi_connector_bundle_ch_filtered_ya_cloud.schoolbook.core.constants import SOURCE_TYPE_CH_SCHOOLBOOK_TABLE
from bi_connector_bundle_ch_filtered_ya_cloud.schoolbook.core.settings import SchoolbookConnectorSettings


class ConnectionClickhouseSchoolbook(ConnectionCHFilteredSubselectByPuidBase[SchoolbookConnectorSettings]):
    source_type = SOURCE_TYPE_CH_SCHOOLBOOK_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_CH_SCHOOLBOOK_TABLE,))
    is_always_internal_source: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True
    settings_type = SchoolbookConnectorSettings
