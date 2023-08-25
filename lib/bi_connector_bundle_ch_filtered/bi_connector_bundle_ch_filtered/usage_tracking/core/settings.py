from bi_configs.connectors_settings import ConnectorSettingsBase, UsageTrackingConnectionSettings
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from bi_configs.settings_loaders.meta_definition import required

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def usage_tracking_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = full_cfg.CONNECTORS_DATA
    return dict(
        USAGE_TRACKING=UsageTrackingConnectionSettings(  # type: ignore
            HOST=cfg.USAGE_TRACKING.CONN_USAGE_TRACKING_HOST,
            PORT=cfg.USAGE_TRACKING.CONN_USAGE_TRACKING_PORT,
            DB_NAME=cfg.USAGE_TRACKING.CONN_USAGE_TRACKING_DB_NAME,
            USERNAME=cfg.USAGE_TRACKING.CONN_USAGE_TRACKING_USERNAME,
            USE_MANAGED_NETWORK=cfg.USAGE_TRACKING.CONN_USAGE_TRACKING_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.USAGE_TRACKING.CONN_USAGE_TRACKING_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.USAGE_TRACKING.CONN_USAGE_TRACKING_SUBSELECT_TEMPLATES),
            REQUIRED_IAM_ROLE=cfg.USAGE_TRACKING.CONN_USAGE_TRACKING_REQUIRED_IAM_ROLE,
            PASSWORD=required(str),
        )
    )


class UsageTrackingSettingDefinition(ConnectorSettingsDefinition):
    settings_class = UsageTrackingConnectionSettings
    fallback = usage_tracking_settings_fallback
