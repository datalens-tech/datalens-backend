from bi_configs.connectors_settings import ConnectorSettingsBase, YQConnectorSettings
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def yq_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = full_cfg.CONNECTORS_DATA
    return dict(
        YQ=YQConnectorSettings(  # type: ignore
            HOST=cfg.YQ.CONN_YQ_HOST,
            PORT=cfg.YQ.CONN_YQ_PORT,
            DB_NAME=cfg.YQ.CONN_YQ_DB_NAME,
        )
    )


class YQSettingDefinition(ConnectorSettingsDefinition):
    settings_class = YQConnectorSettings
    fallback = yq_settings_fallback
