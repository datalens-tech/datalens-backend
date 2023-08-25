from bi_configs.connectors_settings import ConnectorSettingsBase, CHFrozenHorecaConnectorSettings
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from bi_configs.settings_loaders.meta_definition import required

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def ch_frozen_horeca_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = full_cfg.CONNECTORS_DATA
    return dict(
        CH_FROZEN_HORECA=CHFrozenHorecaConnectorSettings(
            HOST=cfg.CH_FROZEN_HORECA.CONN_CH_FROZEN_HORECA_HOST,
            PORT=cfg.CH_FROZEN_HORECA.CONN_CH_FROZEN_HORECA_PORT,
            DB_NAME=cfg.CH_FROZEN_HORECA.CONN_CH_FROZEN_HORECA_DB_MAME,
            USERNAME=cfg.CH_FROZEN_HORECA.CONN_CH_FROZEN_HORECA_USERNAME,
            USE_MANAGED_NETWORK=cfg.CH_FROZEN_HORECA.CONN_CH_FROZEN_HORECA_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CH_FROZEN_HORECA.CONN_CH_FROZEN_HORECA_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CH_FROZEN_HORECA.CONN_CH_FROZEN_HORECA_SUBSELECT_TEMPLATES),
            PASSWORD=required(str),
        )
    )


class CHFrozenHorecaSettingDefinition(ConnectorSettingsDefinition):
    settings_class = CHFrozenHorecaConnectorSettings
    fallback = ch_frozen_horeca_settings_fallback
