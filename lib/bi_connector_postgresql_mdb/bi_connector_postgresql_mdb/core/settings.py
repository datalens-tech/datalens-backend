from dl_configs.connectors_settings import (
    ConnectorsConfigType,
    ConnectorSettingsBase,
)
from dl_core.connectors.settings.primitives import ConnectorSettingsDefinition

from bi_connector_mdb_base.core.settings import MDBConnectorSettings


def postgresql_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    return dict(POSTGRES=MDBConnectorSettings())


class PostgreSQLMDBSettingDefinition(ConnectorSettingsDefinition):
    settings_class = MDBConnectorSettings
    fallback = postgresql_settings_fallback
