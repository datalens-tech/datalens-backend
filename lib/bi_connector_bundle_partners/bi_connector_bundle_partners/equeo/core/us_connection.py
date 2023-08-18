from __future__ import annotations

from typing import TYPE_CHECKING

from bi_connector_bundle_partners.base.core.us_connection import PartnersCHConnectionBase

from bi_connector_bundle_partners.equeo.core.constants import SOURCE_TYPE_EQUEO_CH_TABLE

if TYPE_CHECKING:
    from bi_configs.connectors_settings import EqueoConnectorSettings
    from bi_core.us_manager.us_manager_sync import SyncUSManager


class EqueoCHConnection(PartnersCHConnectionBase):
    source_type = SOURCE_TYPE_EQUEO_CH_TABLE
    allowed_source_types = frozenset((SOURCE_TYPE_EQUEO_CH_TABLE,))

    @property
    def _connector_settings(self) -> EqueoConnectorSettings:
        settings = self._all_connectors_settings.EQUEO
        assert settings is not None
        return settings

    @classmethod
    def _get_connector_settings(cls, usm: SyncUSManager) -> EqueoConnectorSettings:
        settings = cls._get_all_connectors_settings(usm).EQUEO
        assert settings is not None
        return settings
