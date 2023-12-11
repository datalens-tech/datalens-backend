from typing import Optional

import attr

from dl_configs.connectors_settings import (
    ConnectorsConfigType,
    ConnectorSettingsBase,
)
from dl_configs.settings_loaders.meta_definition import s_attrib
from dl_core.connectors.settings.primitives import ConnectorSettingsDefinition


@attr.s(frozen=True)
class YDBConnectorSettings(ConnectorSettingsBase):
    MANAGED_OAUTH_ROW: Optional[str] = False
    DEFAULT_HOST_VALUE: Optional[str] = s_attrib("DEFAULT_HOST_VALUE", missing=None)  # type: ignore


def ydb_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    return dict(YDB=YDBConnectorSettings())


class YDBSettingDefinition(ConnectorSettingsDefinition):
    settings_class = YDBConnectorSettings
    fallback = ydb_settings_fallback
