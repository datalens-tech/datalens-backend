from typing import ClassVar, Optional

from bi_configs.connectors_settings import ConnectorsConfigType, ConnectorSettingsBase, CHFrozenGKHConnectorSettings
from bi_configs.settings_loaders.meta_definition import required
from bi_configs.connectors_data import ConnectorsDataBase

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition, get_connectors_settings_config


class ConnectorsDataCHFrozenGKHBase(ConnectorsDataBase):
    CONN_CH_FROZEN_GKH_HOST: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_GKH_PORT: ClassVar[Optional[int]] = None
    CONN_CH_FROZEN_GKH_DB_MAME: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_GKH_USERNAME: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_GKH_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = None
    CONN_CH_FROZEN_GKH_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = None
    CONN_CH_FROZEN_GKH_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'CH_FROZEN_GKH'


def ch_frozen_gkh_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg, object_like_config_key='CH_FROZEN_GKH', connector_data_class=ConnectorsDataCHFrozenGKHBase,
    )
    if cfg is None:
        return {}
    return dict(
        CH_FROZEN_GKH=CHFrozenGKHConnectorSettings(  # type: ignore
            HOST=cfg.CONN_CH_FROZEN_GKH_HOST,
            PORT=cfg.CONN_CH_FROZEN_GKH_PORT,
            DB_NAME=cfg.CONN_CH_FROZEN_GKH_DB_MAME,
            USERNAME=cfg.CONN_CH_FROZEN_GKH_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_CH_FROZEN_GKH_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_CH_FROZEN_GKH_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CONN_CH_FROZEN_GKH_SUBSELECT_TEMPLATES),  # type: ignore
            PASSWORD=required(str),
        )
    )


class CHFrozenGKHSettingDefinition(ConnectorSettingsDefinition):
    settings_class = CHFrozenGKHConnectorSettings
    fallback = ch_frozen_gkh_settings_fallback
