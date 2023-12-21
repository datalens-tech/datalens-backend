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
from dl_configs.settings_loaders.meta_definition import s_attrib
from dl_core.connectors.settings.primitives import (
    ConnectorSettingsDefinition,
    get_connectors_settings_config,
)


@attr.s(frozen=True)
class YDBConnectorSettings(ConnectorSettingsBase):
    MANAGED_OAUTH_ROW: Optional[bool] = s_attrib("MANAGED_OAUTH_ROW", missing=True)
    DEFAULT_HOST_VALUE: Optional[str] = s_attrib("DEFAULT_HOST_VALUE", missing=None)  # type: ignore


class ConnectorsDataYDBBase(ConnectorsDataBase):
    MANAGED_OAUTH_ROW: ClassVar[Optional[bool]] = True

    @classmethod
    def connector_name(cls) -> str:
        return "YDB"


def ydb_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    # return dict(YDB=YDBConnectorSettings())

    cfg = get_connectors_settings_config(
        full_cfg,
        object_like_config_key="YDB",
        connector_data_class=ConnectorsDataYDBBase,
    )
    if cfg is None:
        return {}
    return dict(YDB=YDBConnectorSettings(MANAGED_OAUTH_ROW=cfg.MANAGED_OAUTH_ROW))  # type: ignore


class YDBSettingDefinition(ConnectorSettingsDefinition):
    settings_class = YDBConnectorSettings
    fallback = ydb_settings_fallback
