from typing import (
    ClassVar,
    Optional,
)

import attr

from dl_configs.connectors_data import ConnectorsDataBase
from dl_configs.connectors_settings import (
    ConnectorsConfigType,
    ConnectorSettingsBase,
)
from dl_configs.settings_loaders.meta_definition import required
from dl_core.connectors.settings.primitives import (
    ConnectorSettingsDefinition,
    get_connectors_settings_config,
)

from bi_connector_bundle_ch_filtered.base.core.settings import ServiceConnectorSettingsBase


@attr.s(frozen=True)
class SMBHeatmapsConnectorSettings(ServiceConnectorSettingsBase):
    """"""


class ConnectorsDataSMBHeatmapsBase(ConnectorsDataBase):
    CONN_SMB_HEATMAPS_HOST: ClassVar[Optional[str]] = None
    CONN_SMB_HEATMAPS_PORT: ClassVar[Optional[int]] = None
    CONN_SMB_HEATMAPS_DB_MAME: ClassVar[Optional[str]] = None
    CONN_SMB_HEATMAPS_USERNAME: ClassVar[Optional[str]] = None
    CONN_SMB_HEATMAPS_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = None
    CONN_SMB_HEATMAPS_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = None
    CONN_SMB_HEATMAPS_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = None

    @classmethod
    def connector_name(cls) -> str:
        return "SMB_HEATMAPS"


def smb_heatmaps_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg,
        object_like_config_key="SMB_HEATMAPS",
        connector_data_class=ConnectorsDataSMBHeatmapsBase,
    )
    if cfg is None:
        return {}
    return dict(
        SMB_HEATMAPS=SMBHeatmapsConnectorSettings(  # type: ignore
            HOST=cfg.CONN_SMB_HEATMAPS_HOST,
            PORT=cfg.CONN_SMB_HEATMAPS_PORT,
            DB_NAME=cfg.CONN_SMB_HEATMAPS_DB_MAME,
            USERNAME=cfg.CONN_SMB_HEATMAPS_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_SMB_HEATMAPS_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_SMB_HEATMAPS_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CONN_SMB_HEATMAPS_SUBSELECT_TEMPLATES),  # type: ignore
            PASSWORD=required(str),
        )
    )


class CHSMBHeatmapsSettingDefinition(ConnectorSettingsDefinition):
    settings_class = SMBHeatmapsConnectorSettings
    fallback = smb_heatmaps_settings_fallback
