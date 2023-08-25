from bi_configs.connectors_settings import ConnectorSettingsBase, BillingConnectorSettings
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from bi_configs.settings_loaders.meta_definition import required

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def ch_billing_analytics_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = full_cfg.CONNECTORS_DATA
    return dict(
        CH_BILLING_ANALYTICS=BillingConnectorSettings(  # type: ignore
            HOST=cfg.CH_BILLING_ANALYTICS.CONN_BILLING_HOST,
            PORT=cfg.CH_BILLING_ANALYTICS.CONN_BILLING_PORT,
            DB_NAME=cfg.CH_BILLING_ANALYTICS.CONN_BILLING_DB_MAME,
            USERNAME=cfg.CH_BILLING_ANALYTICS.CONN_BILLING_USERNAME,
            USE_MANAGED_NETWORK=cfg.CH_BILLING_ANALYTICS.CONN_BILLING_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CH_BILLING_ANALYTICS.CONN_BILLING_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CH_BILLING_ANALYTICS.CONN_BILLING_SUBSELECT_TEMPLATES),
            PASSWORD=required(str),
        )
    )


class BillingAnalyticsSettingDefinition(ConnectorSettingsDefinition):
    settings_class = BillingConnectorSettings
    fallback = ch_billing_analytics_settings_fallback
