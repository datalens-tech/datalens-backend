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
from dl_configs.settings_loaders.meta_definition import required
from dl_core.connectors.settings.primitives import (
    ConnectorSettingsDefinition,
    get_connectors_settings_config,
)

from bi_connector_bundle_ch_filtered.base.core.settings import ServiceConnectorSettingsBase


@attr.s(frozen=True)
class BillingConnectorSettings(ServiceConnectorSettingsBase):
    ALLOW_PUBLIC_USAGE: bool = False


class ConnectorsDataBillingBase(ConnectorsDataBase):
    CONN_BILLING_HOST: ClassVar[Optional[str]] = None
    CONN_BILLING_PORT: ClassVar[Optional[int]] = None
    CONN_BILLING_DB_MAME: ClassVar[Optional[str]] = None
    CONN_BILLING_USERNAME: ClassVar[Optional[str]] = None
    CONN_BILLING_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = None
    CONN_BILLING_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = None
    CONN_BILLING_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = None

    @classmethod
    def connector_name(cls) -> str:
        return "CH_BILLING_ANALYTICS"


def ch_billing_analytics_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg,
        object_like_config_key="CH_BILLING_ANALYTICS",
        connector_data_class=ConnectorsDataBillingBase,
    )
    if cfg is None:
        return {}
    return dict(
        CH_BILLING_ANALYTICS=BillingConnectorSettings(  # type: ignore
            HOST=cfg.CONN_BILLING_HOST,
            PORT=cfg.CONN_BILLING_PORT,
            DB_NAME=cfg.CONN_BILLING_DB_MAME,
            USERNAME=cfg.CONN_BILLING_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_BILLING_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_BILLING_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CONN_BILLING_SUBSELECT_TEMPLATES),  # type: ignore
            PASSWORD=required(str),
        )
    )


class BillingAnalyticsSettingDefinition(ConnectorSettingsDefinition):
    settings_class = BillingConnectorSettings
    fallback = ch_billing_analytics_settings_fallback
