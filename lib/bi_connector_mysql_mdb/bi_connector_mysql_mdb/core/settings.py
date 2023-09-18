import attr

from dl_configs.connectors_settings import ConnectorsConfigType, ConnectorSettingsBase
from dl_configs.settings_loaders.meta_definition import s_attrib

from dl_core.connectors.settings.primitives import ConnectorSettingsDefinition


@attr.s(frozen=True)
class MysqlConnectorSettings(ConnectorSettingsBase):
    USE_MDB_CLUSTER_PICKER: bool = s_attrib("USE_MDB_CLUSTER_PICKER", missing=False)  # type: ignore


def mysql_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    return dict(MYSQL=MysqlConnectorSettings())


class MySQLMDBSettingDefinition(ConnectorSettingsDefinition):
    settings_class = MysqlConnectorSettings
    fallback = mysql_settings_fallback
