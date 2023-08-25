from bi_configs.connectors_settings import ConnectorSettingsBase, CHFrozenTransparencyConnectorSettings
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from bi_configs.settings_loaders.meta_definition import required

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def ch_frozen_transparency_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = full_cfg.CONNECTORS_DATA
    return dict(
        CH_FROZEN_TRANSPARENCY=CHFrozenTransparencyConnectorSettings(
            HOST=cfg.CH_FROZEN_TRANSPARENCY.CONN_CH_FROZEN_TRANSPARENCY_HOST,
            PORT=cfg.CH_FROZEN_TRANSPARENCY.CONN_CH_FROZEN_TRANSPARENCY_PORT,
            DB_NAME=cfg.CH_FROZEN_TRANSPARENCY.CONN_CH_FROZEN_TRANSPARENCY_DB_MAME,
            USERNAME=cfg.CH_FROZEN_TRANSPARENCY.CONN_CH_FROZEN_TRANSPARENCY_USERNAME,
            USE_MANAGED_NETWORK=cfg.CH_FROZEN_TRANSPARENCY.CONN_CH_FROZEN_TRANSPARENCY_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CH_FROZEN_TRANSPARENCY.CONN_CH_FROZEN_TRANSPARENCY_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CH_FROZEN_TRANSPARENCY.CONN_CH_FROZEN_TRANSPARENCY_SUBSELECT_TEMPLATES),
            PASSWORD=required(str),
        )
    )


class CHFrozenTransparencySettingDefinition(ConnectorSettingsDefinition):
    settings_class = CHFrozenTransparencyConnectorSettings
    fallback = ch_frozen_transparency_settings_fallback
