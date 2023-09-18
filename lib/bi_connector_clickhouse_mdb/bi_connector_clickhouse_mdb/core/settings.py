import attr

from dl_configs.connectors_settings import ConnectorsConfigType, ConnectorSettingsBase
from dl_configs.settings_loaders.meta_definition import s_attrib

from dl_core.connectors.settings.primitives import ConnectorSettingsDefinition


@attr.s(frozen=True)
class ClickHouseConnectorSettings(ConnectorSettingsBase):
    USE_MDB_CLUSTER_PICKER: bool = s_attrib("USE_MDB_CLUSTER_PICKER", missing=False)  # type: ignore


def clickhouse_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    return dict(CLICKHOUSE=ClickHouseConnectorSettings())


class ClickHouseMDBSettingDefinition(ConnectorSettingsDefinition):
    settings_class = ClickHouseConnectorSettings
    fallback = clickhouse_settings_fallback
