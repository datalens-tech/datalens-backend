from typing import ClassVar, Optional

from bi_configs.connectors_settings import ConnectorsConfigType, ConnectorSettingsBase, MonitoringConnectorSettings
from bi_configs.connectors_data import ConnectorsDataBase

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition, get_connectors_settings_config


class ConnectorsDataMonitoringBase(ConnectorsDataBase):
    CONN_MONITORING_HOST: ClassVar[Optional[str]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'MONITORING'


def monitoring_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg, object_like_config_key='MONITORING', connector_data_class=ConnectorsDataMonitoringBase,
    )
    if cfg is None:
        return {}
    return dict(
        MONITORING=MonitoringConnectorSettings(  # type: ignore
            HOST=cfg.CONN_MONITORING_HOST,
        )
    )


class MonitoringSettingDefinition(ConnectorSettingsDefinition):
    settings_class = MonitoringConnectorSettings
    fallback = monitoring_settings_fallback
