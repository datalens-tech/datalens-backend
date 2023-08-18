from __future__ import annotations

from typing import ClassVar, TYPE_CHECKING

from bi_connector_bundle_ch_filtered_ya_cloud.schoolbook.core.constants import (
    SOURCE_TYPE_CH_SCHOOLBOOK_TABLE,
)
from bi_connector_bundle_ch_filtered_ya_cloud.base.core.us_connection import ConnectionCHFilteredSubselectByPuidBase

if TYPE_CHECKING:
    from bi_configs.connectors_settings import SchoolbookConnectorSettings


class ConnectionClickhouseSchoolbook(ConnectionCHFilteredSubselectByPuidBase):
    source_type = SOURCE_TYPE_CH_SCHOOLBOOK_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_CH_SCHOOLBOOK_TABLE,))
    is_always_internal_source: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True

    @property
    def _connector_settings(self) -> SchoolbookConnectorSettings:
        settings = self._all_connectors_settings.SCHOOLBOOK
        assert settings is not None
        return settings
