import attr

from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_configs.settings_loaders.meta_definition import s_attrib


@attr.s(frozen=True)
class MDBConnectorSettings(ConnectorSettingsBase):
    USE_MDB_CLUSTER_PICKER: bool = s_attrib("USE_MDB_CLUSTER_PICKER", missing=False)  # type: ignore
    MDB_ORG_CHECK_ENABLED: bool = s_attrib("MDB_ORG_CHECK_ENABLED", missing=False)  # type: ignore
