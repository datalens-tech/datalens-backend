from typing import ClassVar, Optional

from bi_configs.connectors_settings import (
    ConnectorsConfigType, ConnectorSettingsBase, MoySkladConnectorSettings, PartnerKeys,
)
from bi_configs.settings_loaders.meta_definition import required
from bi_configs.connectors_data import ConnectorsDataBase

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition, get_connectors_settings_config


class ConnectorsDataMoyskladBase(ConnectorsDataBase):
    CONN_MOYSKLAD_HOST: ClassVar[Optional[str]] = None
    CONN_MOYSKLAD_PORT: ClassVar[Optional[int]] = None
    CONN_MOYSKLAD_USERNAME: ClassVar[Optional[str]] = None
    CONN_MOYSKLAD_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'MOYSKLAD'


def moysklad_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg, object_like_config_key='MOYSKLAD', connector_data_class=ConnectorsDataMoyskladBase,
    )
    if cfg is None:
        return {}
    return dict(
        MOYSKLAD=MoySkladConnectorSettings(  # type: ignore
            HOST=cfg.CONN_MOYSKLAD_HOST,
            PORT=cfg.CONN_MOYSKLAD_PORT,
            USERNAME=cfg.CONN_MOYSKLAD_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_MOYSKLAD_USE_MANAGED_NETWORK,
            PASSWORD=required(str),
            PARTNER_KEYS=required(PartnerKeys),
        )
    )


class MoySkladSettingDefinition(ConnectorSettingsDefinition):
    settings_class = MoySkladConnectorSettings
    fallback = moysklad_settings_fallback
