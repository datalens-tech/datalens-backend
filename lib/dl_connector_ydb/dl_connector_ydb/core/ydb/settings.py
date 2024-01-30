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
    HAS_AUTH: Optional[bool] = s_attrib("HAS_AUTH", missing=True)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "bool | None")  [assignment]
    DEFAULT_HOST_VALUE: Optional[str] = s_attrib("DEFAULT_HOST_VALUE", missing=None)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str | None")  [assignment]


class ConnectorsDataYDBBase(ConnectorsDataBase):
    HAS_AUTH: ClassVar[Optional[bool]] = True

    @classmethod
    def connector_name(cls) -> str:
        return "YDB"


def ydb_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg,
        object_like_config_key="YDB",
        connector_data_class=ConnectorsDataYDBBase,
    )
    if cfg is None:
        return {}
    return dict(YDB=YDBConnectorSettings(HAS_AUTH=cfg.HAS_AUTH))  # type: ignore  # 2024-01-30 # TODO: Unexpected keyword argument "HAS_AUTH" for "YDBConnectorSettings"  [call-arg]


class YDBSettingDefinition(ConnectorSettingsDefinition):
    settings_class = YDBConnectorSettings
    fallback = ydb_settings_fallback
