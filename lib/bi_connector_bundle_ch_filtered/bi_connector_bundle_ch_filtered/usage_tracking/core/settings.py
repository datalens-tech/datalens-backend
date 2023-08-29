from bi_configs.connectors_settings import ConnectorsConfigType, ConnectorSettingsBase, UsageTrackingConnectionSettings
from bi_configs.settings_loaders.meta_definition import required
from bi_defaults.connectors_data import ConnectorsDataUsageTrackingBase

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition, get_connectors_settings_config


def usage_tracking_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg, object_like_config_key='USAGE_TRACKING', connector_data_class=ConnectorsDataUsageTrackingBase,
    )
    if cfg is None:
        return {}
    return dict(
        USAGE_TRACKING=UsageTrackingConnectionSettings(  # type: ignore
            HOST=cfg.CONN_USAGE_TRACKING_HOST,
            PORT=cfg.CONN_USAGE_TRACKING_PORT,
            DB_NAME=cfg.CONN_USAGE_TRACKING_DB_NAME,
            USERNAME=cfg.CONN_USAGE_TRACKING_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_USAGE_TRACKING_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_USAGE_TRACKING_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CONN_USAGE_TRACKING_SUBSELECT_TEMPLATES),  # type: ignore
            REQUIRED_IAM_ROLE=cfg.CONN_USAGE_TRACKING_REQUIRED_IAM_ROLE,
            PASSWORD=required(str),
        )
    )


class UsageTrackingSettingDefinition(ConnectorSettingsDefinition):
    settings_class = UsageTrackingConnectionSettings
    fallback = usage_tracking_settings_fallback
