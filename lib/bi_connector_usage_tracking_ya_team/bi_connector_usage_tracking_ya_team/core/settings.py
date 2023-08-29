from bi_configs.connectors_settings import (
    ConnectorsConfigType, ConnectorSettingsBase, UsageTrackingYaTeamConnectionSettings,
)
from bi_configs.settings_loaders.meta_definition import required
from bi_configs.connectors_data import ConnectorsDataUsageTrackingYaTeamBase

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition, get_connectors_settings_config


def usage_tracking_ya_team_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg, object_like_config_key='USAGE_TRACKING_YA_TEAM',
        connector_data_class=ConnectorsDataUsageTrackingYaTeamBase,
    )
    if cfg is None:
        return {}
    return dict(
        USAGE_TRACKING_YA_TEAM=UsageTrackingYaTeamConnectionSettings(  # type: ignore
            HOST=cfg.CONN_USAGE_TRACKING_YA_TEAM_HOST,
            PORT=cfg.CONN_USAGE_TRACKING_YA_TEAM_PORT,
            DB_NAME=cfg.CONN_USAGE_TRACKING_YA_TEAM_DB_NAME,
            USERNAME=cfg.CONN_USAGE_TRACKING_YA_TEAM_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_USAGE_TRACKING_YA_TEAM_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_USAGE_TRACKING_YA_TEAM_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CONN_USAGE_TRACKING_YA_TEAM_SUBSELECT_TEMPLATES),  # type: ignore
            MAX_EXECUTION_TIME=cfg.CONN_USAGE_TRACKING_YA_TEAM_MAX_EXECUTION_TIME,
            PASSWORD=required(str),
        )
    )


class UsageTrackingYaTeamSettingDefinition(ConnectorSettingsDefinition):
    settings_class = UsageTrackingYaTeamConnectionSettings
    fallback = usage_tracking_ya_team_settings_fallback
