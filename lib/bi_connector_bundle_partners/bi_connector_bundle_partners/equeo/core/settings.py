from bi_configs.connectors_settings import (
    ConnectorsConfigType, ConnectorSettingsBase, EqueoConnectorSettings, PartnerKeys,
)
from bi_configs.settings_loaders.meta_definition import required
from bi_defaults.connectors_data import ConnectorsDataEqueoBase

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition, get_connectors_settings_config


def equeo_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg, object_like_config_key='EQUEO', connector_data_class=ConnectorsDataEqueoBase,
    )
    if cfg is None:
        return {}
    return dict(
        EQUEO=EqueoConnectorSettings(  # type: ignore
            HOST=cfg.CONN_EQUEO_HOST,
            PORT=cfg.CONN_EQUEO_PORT,
            USERNAME=cfg.CONN_EQUEO_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_EQUEO_USE_MANAGED_NETWORK,
            PASSWORD=required(str),
            PARTNER_KEYS=required(PartnerKeys),
        )
    )


class EqueoSettingDefinition(ConnectorSettingsDefinition):
    settings_class = EqueoConnectorSettings
    fallback = equeo_settings_fallback
