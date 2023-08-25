from bi_configs.connectors_settings import ConnectorSettingsBase, MonitoringConnectorSettings
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def monitoring_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = full_cfg.CONNECTORS_DATA
    return dict(
        MONITORING=MonitoringConnectorSettings(  # type: ignore
            HOST=cfg.MONITORING.CONN_MONITORING_HOST,
        )
    )


class MonitoringSettingDefinition(ConnectorSettingsDefinition):
    settings_class = MonitoringConnectorSettings
    fallback = monitoring_settings_fallback
