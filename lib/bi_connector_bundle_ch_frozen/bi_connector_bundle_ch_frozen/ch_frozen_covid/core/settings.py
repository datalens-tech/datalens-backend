from bi_configs.connectors_settings import ConnectorSettingsBase, CHFrozenCovidConnectorSettings
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from bi_configs.settings_loaders.meta_definition import required

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def ch_frozen_covid_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = full_cfg.CONNECTORS_DATA
    return dict(
        CH_FROZEN_COVID=CHFrozenCovidConnectorSettings(
            HOST=cfg.CH_FROZEN_COVID.CONN_CH_FROZEN_COVID_HOST,
            PORT=cfg.CH_FROZEN_COVID.CONN_CH_FROZEN_COVID_PORT,
            DB_NAME=cfg.CH_FROZEN_COVID.CONN_CH_FROZEN_COVID_DB_MAME,
            USERNAME=cfg.CH_FROZEN_COVID.CONN_CH_FROZEN_COVID_USERNAME,
            USE_MANAGED_NETWORK=cfg.CH_FROZEN_COVID.CONN_CH_FROZEN_COVID_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CH_FROZEN_COVID.CONN_CH_FROZEN_COVID_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CH_FROZEN_COVID.CONN_CH_FROZEN_COVID_SUBSELECT_TEMPLATES),
            PASSWORD=required(str),
        )
    )


class CHFrozenCovidSettingDefinition(ConnectorSettingsDefinition):
    settings_class = CHFrozenCovidConnectorSettings
    fallback = ch_frozen_covid_settings_fallback
