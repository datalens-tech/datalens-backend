from bi_configs.connectors_settings import ConnectorSettingsBase, CHFrozenSamplesConnectorSettings
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from bi_configs.settings_loaders.meta_definition import required

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def ch_frozen_samples_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = full_cfg.CONNECTORS_DATA
    return dict(
        CH_FROZEN_SAMPLES=CHFrozenSamplesConnectorSettings(
            HOST=cfg.CH_FROZEN_SAMPLES.CONN_CH_FROZEN_SAMPLES_HOST,
            PORT=cfg.CH_FROZEN_SAMPLES.CONN_CH_FROZEN_SAMPLES_PORT,
            DB_NAME=cfg.CH_FROZEN_SAMPLES.CONN_CH_FROZEN_SAMPLES_DB_MAME,
            USERNAME=cfg.CH_FROZEN_SAMPLES.CONN_CH_FROZEN_SAMPLES_USERNAME,
            USE_MANAGED_NETWORK=cfg.CH_FROZEN_SAMPLES.CONN_CH_FROZEN_SAMPLES_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CH_FROZEN_SAMPLES.CONN_CH_FROZEN_SAMPLES_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CH_FROZEN_SAMPLES.CONN_CH_FROZEN_SAMPLES_SUBSELECT_TEMPLATES),
            PASSWORD=required(str),
        )
    )


class CHFrozenSamplesSettingDefinition(ConnectorSettingsDefinition):
    settings_class = CHFrozenSamplesConnectorSettings
    fallback = ch_frozen_samples_settings_fallback
