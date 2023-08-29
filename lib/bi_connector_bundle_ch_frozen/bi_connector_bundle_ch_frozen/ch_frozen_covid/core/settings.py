from bi_configs.connectors_settings import ConnectorsConfigType, ConnectorSettingsBase, CHFrozenCovidConnectorSettings
from bi_configs.settings_loaders.meta_definition import required
from bi_configs.connectors_data import ConnectorsDataCHFrozenCovidBase

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition, get_connectors_settings_config


def ch_frozen_covid_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg, object_like_config_key='CH_FROZEN_COVID', connector_data_class=ConnectorsDataCHFrozenCovidBase,
    )
    if cfg is None:
        return {}
    return dict(
        CH_FROZEN_COVID=CHFrozenCovidConnectorSettings(  # type: ignore
            HOST=cfg.CONN_CH_FROZEN_COVID_HOST,
            PORT=cfg.CONN_CH_FROZEN_COVID_PORT,
            DB_NAME=cfg.CONN_CH_FROZEN_COVID_DB_MAME,
            USERNAME=cfg.CONN_CH_FROZEN_COVID_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_CH_FROZEN_COVID_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_CH_FROZEN_COVID_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CONN_CH_FROZEN_COVID_SUBSELECT_TEMPLATES),  # type: ignore
            PASSWORD=required(str),
        )
    )


class CHFrozenCovidSettingDefinition(ConnectorSettingsDefinition):
    settings_class = CHFrozenCovidConnectorSettings
    fallback = ch_frozen_covid_settings_fallback
