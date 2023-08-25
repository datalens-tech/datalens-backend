from bi_configs.connectors_settings import ConnectorSettingsBase, EqueoConnectorSettings, PartnerKeys
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from bi_configs.settings_loaders.meta_definition import required

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def equeo_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = full_cfg.CONNECTORS_DATA
    return dict(
        EQUEO=EqueoConnectorSettings(  # type: ignore
            HOST=cfg.EQUEO.CONN_EQUEO_HOST,
            PORT=cfg.EQUEO.CONN_EQUEO_PORT,
            USERNAME=cfg.EQUEO.CONN_EQUEO_USERNAME,
            USE_MANAGED_NETWORK=cfg.EQUEO.CONN_EQUEO_USE_MANAGED_NETWORK,
            PASSWORD=required(str),
            PARTNER_KEYS=required(PartnerKeys),
        )
    )


class EqueoSettingDefinition(ConnectorSettingsDefinition):
    settings_class = EqueoConnectorSettings
    fallback = equeo_settings_fallback
