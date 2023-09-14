from typing import ClassVar, Optional

import attr

from bi_configs.connectors_data import ConnectorsDataBase
from bi_configs.connectors_settings import ConnectorsConfigType, ConnectorSettingsBase
from bi_configs.settings_loaders.meta_definition import required

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition, get_connectors_settings_config

from bi_connector_bundle_partners.base.core.settings import PartnerConnectorSettingsBase, PartnerKeys


@attr.s(frozen=True)
class EqueoConnectorSettings(PartnerConnectorSettingsBase):
    """"""


class ConnectorsDataEqueoBase(ConnectorsDataBase):
    CONN_EQUEO_HOST: ClassVar[Optional[str]] = None
    CONN_EQUEO_PORT: ClassVar[Optional[int]] = None
    CONN_EQUEO_USERNAME: ClassVar[Optional[str]] = None
    CONN_EQUEO_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'EQUEO'


def equeo_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg, object_like_config_key='EQUEO', connector_data_class=ConnectorsDataEqueoBase,
    )
    if cfg is None:
        return {}
    return dict(
        EQUEO=EqueoConnectorSettings(  # type: ignore
            HOST=cfg.CONN_EQUEO_HOST,
            PORT=cfg.CONN_EQUEO_PORT,
            USERNAME=cfg.CONN_EQUEO_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_EQUEO_USE_MANAGED_NETWORK,
            PASSWORD=required(str),
            PARTNER_KEYS=required(PartnerKeys),
        )
    )


class EqueoSettingDefinition(ConnectorSettingsDefinition):
    settings_class = EqueoConnectorSettings
    fallback = equeo_settings_fallback
