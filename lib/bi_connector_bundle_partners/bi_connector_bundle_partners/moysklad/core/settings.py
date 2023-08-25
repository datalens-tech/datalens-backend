from bi_configs.connectors_settings import ConnectorSettingsBase, MoySkladConnectorSettings, PartnerKeys
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from bi_configs.settings_loaders.meta_definition import required

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def moysklad_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = full_cfg.CONNECTORS_DATA
    return dict(
        MOYSKLAD=MoySkladConnectorSettings(  # type: ignore
            HOST=cfg.MOYSKLAD.CONN_MOYSKLAD_HOST,
            PORT=cfg.MOYSKLAD.CONN_MOYSKLAD_PORT,
            USERNAME=cfg.MOYSKLAD.CONN_MOYSKLAD_USERNAME,
            USE_MANAGED_NETWORK=cfg.MOYSKLAD.CONN_MOYSKLAD_USE_MANAGED_NETWORK,
            PASSWORD=required(str),
            PARTNER_KEYS=required(PartnerKeys),
        )
    )


class MoySkladSettingDefinition(ConnectorSettingsDefinition):
    settings_class = MoySkladConnectorSettings
    fallback = moysklad_settings_fallback
