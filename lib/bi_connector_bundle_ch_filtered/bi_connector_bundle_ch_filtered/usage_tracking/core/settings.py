from typing import (
    ClassVar,
    Optional,
)

import attr

from dl_configs.connectors_data import ConnectorsDataBase
from dl_configs.connectors_settings import (
    ConnectorsConfigType,
    ConnectorSettingsBase,
)
from dl_configs.settings_loaders.meta_definition import (
    required,
    s_attrib,
)
from dl_core.connectors.settings.primitives import (
    ConnectorSettingsDefinition,
    get_connectors_settings_config,
)

from bi_connector_bundle_ch_filtered.base.core.settings import ServiceConnectorSettingsBase


@attr.s(frozen=True)
class UsageTrackingConnectionSettings(ServiceConnectorSettingsBase):
    REQUIRED_IAM_ROLE: str = s_attrib("REQUIRED_IAM_ROLE", missing=None)  # type: ignore


class ConnectorsDataUsageTrackingBase(ConnectorsDataBase):
    CONN_USAGE_TRACKING_HOST: ClassVar[Optional[str]] = None
    CONN_USAGE_TRACKING_PORT: ClassVar[Optional[int]] = None
    CONN_USAGE_TRACKING_DB_NAME: ClassVar[Optional[str]] = None
    CONN_USAGE_TRACKING_USERNAME: ClassVar[Optional[str]] = None
    CONN_USAGE_TRACKING_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = None
    CONN_USAGE_TRACKING_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = None
    CONN_USAGE_TRACKING_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = None
    CONN_USAGE_TRACKING_REQUIRED_IAM_ROLE: ClassVar[Optional[str]] = None

    @classmethod
    def connector_name(self) -> str:
        return "USAGE_TRACKING"


def usage_tracking_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg,
        object_like_config_key="USAGE_TRACKING",
        connector_data_class=ConnectorsDataUsageTrackingBase,
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
