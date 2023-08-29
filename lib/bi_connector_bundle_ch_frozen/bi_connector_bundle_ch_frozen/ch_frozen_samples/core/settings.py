from bi_configs.connectors_settings import ConnectorsConfigType, ConnectorSettingsBase, CHFrozenSamplesConnectorSettings
from bi_configs.settings_loaders.meta_definition import required
from bi_defaults.connectors_data import ConnectorsDataCHFrozenSamplesBase

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition, get_connectors_settings_config


def ch_frozen_samples_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg, object_like_config_key='CH_FROZEN_SAMPLES', connector_data_class=ConnectorsDataCHFrozenSamplesBase,
    )
    if cfg is None:
        return {}
    return dict(
        CH_FROZEN_SAMPLES=CHFrozenSamplesConnectorSettings(  # type: ignore
            HOST=cfg.CONN_CH_FROZEN_SAMPLES_HOST,
            PORT=cfg.CONN_CH_FROZEN_SAMPLES_PORT,
            DB_NAME=cfg.CONN_CH_FROZEN_SAMPLES_DB_MAME,
            USERNAME=cfg.CONN_CH_FROZEN_SAMPLES_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_CH_FROZEN_SAMPLES_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_CH_FROZEN_SAMPLES_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CONN_CH_FROZEN_SAMPLES_SUBSELECT_TEMPLATES),  # type: ignore
            PASSWORD=required(str),
        )
    )


class CHFrozenSamplesSettingDefinition(ConnectorSettingsDefinition):
    settings_class = CHFrozenSamplesConnectorSettings
    fallback = ch_frozen_samples_settings_fallback
