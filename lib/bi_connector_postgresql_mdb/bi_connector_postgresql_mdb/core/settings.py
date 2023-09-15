import attr

from bi_configs.connectors_settings import ConnectorsConfigType, ConnectorSettingsBase
from bi_configs.settings_loaders.meta_definition import s_attrib

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


@attr.s(frozen=True)
class PostgresConnectorSettings(ConnectorSettingsBase):
    USE_MDB_CLUSTER_PICKER: bool = s_attrib("USE_MDB_CLUSTER_PICKER", missing=False)  # type: ignore


def postgresql_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    return dict(POSTGRES=PostgresConnectorSettings())


class PostgreSQLMDBSettingDefinition(ConnectorSettingsDefinition):
    settings_class = PostgresConnectorSettings
    fallback = postgresql_settings_fallback
