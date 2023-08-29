from bi_configs.connectors_settings import ConnectorsConfigType, ConnectorSettingsBase, YQConnectorSettings
from bi_defaults.connectors_data import ConnectorsDataYQBase

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition, get_connectors_settings_config


def yq_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg, object_like_config_key='YQ', connector_data_class=ConnectorsDataYQBase,
    )
    if cfg is None:
        return {}
    return dict(
        YQ=YQConnectorSettings(  # type: ignore
            HOST=cfg.CONN_YQ_HOST,
            PORT=cfg.CONN_YQ_PORT,
            DB_NAME=cfg.CONN_YQ_DB_NAME,
        )
    )


class YQSettingDefinition(ConnectorSettingsDefinition):
    settings_class = YQConnectorSettings
    fallback = yq_settings_fallback
