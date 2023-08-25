from bi_configs.connectors_settings import ConnectorSettingsBase, UsageTrackingYaTeamConnectionSettings
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from bi_configs.settings_loaders.meta_definition import required

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def usage_tracking_ya_team_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = full_cfg.CONNECTORS_DATA
    return dict(
        USAGE_TRACKING_YA_TEAM=UsageTrackingYaTeamConnectionSettings(  # type: ignore
            HOST=cfg.USAGE_TRACKING_YA_TEAM.CONN_USAGE_TRACKING_YA_TEAM_HOST,
            PORT=cfg.USAGE_TRACKING_YA_TEAM.CONN_USAGE_TRACKING_YA_TEAM_PORT,
            DB_NAME=cfg.USAGE_TRACKING_YA_TEAM.CONN_USAGE_TRACKING_YA_TEAM_DB_NAME,
            USERNAME=cfg.USAGE_TRACKING_YA_TEAM.CONN_USAGE_TRACKING_YA_TEAM_USERNAME,
            USE_MANAGED_NETWORK=cfg.USAGE_TRACKING_YA_TEAM.CONN_USAGE_TRACKING_YA_TEAM_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.USAGE_TRACKING_YA_TEAM.CONN_USAGE_TRACKING_YA_TEAM_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.USAGE_TRACKING_YA_TEAM.CONN_USAGE_TRACKING_YA_TEAM_SUBSELECT_TEMPLATES),
            MAX_EXECUTION_TIME=cfg.USAGE_TRACKING_YA_TEAM.CONN_USAGE_TRACKING_YA_TEAM_MAX_EXECUTION_TIME,
            PASSWORD=required(str),
        )
    )


class UsageTrackingYaTeamSettingDefinition(ConnectorSettingsDefinition):
    settings_class = UsageTrackingYaTeamConnectionSettings
    fallback = usage_tracking_ya_team_settings_fallback
