from bi_configs.connectors_settings import ConnectorSettingsBase, CHFrozenWeatherConnectorSettings
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from bi_configs.settings_loaders.meta_definition import required

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def ch_frozen_weather_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = full_cfg.CONNECTORS_DATA
    return dict(
        CH_FROZEN_WEATHER=CHFrozenWeatherConnectorSettings(
            HOST=cfg.CH_FROZEN_WEATHER.CONN_CH_FROZEN_WEATHER_HOST,
            PORT=cfg.CH_FROZEN_WEATHER.CONN_CH_FROZEN_WEATHER_PORT,
            DB_NAME=cfg.CH_FROZEN_WEATHER.CONN_CH_FROZEN_WEATHER_DB_MAME,
            USERNAME=cfg.CH_FROZEN_WEATHER.CONN_CH_FROZEN_WEATHER_USERNAME,
            USE_MANAGED_NETWORK=cfg.CH_FROZEN_WEATHER.CONN_CH_FROZEN_WEATHER_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CH_FROZEN_WEATHER.CONN_CH_FROZEN_WEATHER_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CH_FROZEN_WEATHER.CONN_CH_FROZEN_WEATHER_SUBSELECT_TEMPLATES),
            PASSWORD=required(str),
        )
    )


class CHFrozenWeatherSettingDefinition(ConnectorSettingsDefinition):
    settings_class = CHFrozenWeatherConnectorSettings
    fallback = ch_frozen_weather_settings_fallback
