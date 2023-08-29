from bi_configs.connectors_settings import ConnectorsConfigType, ConnectorSettingsBase, BillingConnectorSettings
from bi_configs.settings_loaders.meta_definition import required
from bi_defaults.connectors_data import ConnectorsDataBillingBase

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition, get_connectors_settings_config


def ch_billing_analytics_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg, object_like_config_key='CH_BILLING_ANALYTICS', connector_data_class=ConnectorsDataBillingBase,
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
