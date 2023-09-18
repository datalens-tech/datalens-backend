from typing import ClassVar, Optional

import attr

from dl_configs.connectors_data import ConnectorsDataBase
from dl_configs.connectors_settings import ConnectorsConfigType, ConnectorSettingsBase
from dl_configs.settings_loaders.meta_definition import required, s_attrib

from dl_core.connectors.settings.primitives import ConnectorSettingsDefinition, get_connectors_settings_config

from bi_connector_bundle_ch_filtered.base.core.settings import ServiceConnectorSettingsBase


@attr.s(frozen=True)
class UsageTrackingYaTeamConnectionSettings(ServiceConnectorSettingsBase):
    MAX_EXECUTION_TIME: int = s_attrib("MAX_EXECUTION_TIME", missing=None)  # type: ignore


class ConnectorsDataUsageTrackingYaTeamBase(ConnectorsDataBase):
    CONN_USAGE_TRACKING_YA_TEAM_HOST: ClassVar[Optional[str]] = None
    CONN_USAGE_TRACKING_YA_TEAM_PORT: ClassVar[Optional[int]] = None
    CONN_USAGE_TRACKING_YA_TEAM_DB_NAME: ClassVar[Optional[str]] = None
    CONN_USAGE_TRACKING_YA_TEAM_USERNAME: ClassVar[Optional[str]] = None
    CONN_USAGE_TRACKING_YA_TEAM_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = None
    CONN_USAGE_TRACKING_YA_TEAM_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = None
    CONN_USAGE_TRACKING_YA_TEAM_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = None
    CONN_USAGE_TRACKING_YA_TEAM_MAX_EXECUTION_TIME: ClassVar[Optional[int]] = None

    @classmethod
    def connector_name(self) -> str:
        return 'USAGE_TRACKING_YA_TEAM'


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
