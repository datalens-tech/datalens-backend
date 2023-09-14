from typing import Optional

import attr

from bi_configs.connectors_settings import ConnectorsConfigType, ConnectorSettingsBase
from bi_configs.settings_loaders.meta_definition import s_attrib

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


@attr.s(frozen=True)
class YDBConnectorSettings(ConnectorSettingsBase):
    USE_MDB_CLUSTER_PICKER: bool = s_attrib("USE_MDB_CLUSTER_PICKER", missing=False)  # type: ignore
    DEFAULT_HOST_VALUE: Optional[str] = s_attrib("DEFAULT_HOST_VALUE", missing=None)  # type: ignore


def ydb_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    return dict(YDB=YDBConnectorSettings())


class YDBSettingDefinition(ConnectorSettingsDefinition):
    settings_class = YDBConnectorSettings
    fallback = ydb_settings_fallback
