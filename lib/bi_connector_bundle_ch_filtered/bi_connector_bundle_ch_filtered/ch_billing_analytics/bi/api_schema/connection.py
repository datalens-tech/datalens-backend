from __future__ import annotations

from bi_connector_bundle_ch_filtered.ch_billing_analytics.core.us_connection import BillingAnalyticsCHConnection

from dl_api_connector.api_schema.connection_base import ConnectionSchema, ConnectionMetaMixin


class CHBillingAnalyticsConnectionSchema(ConnectionMetaMixin, ConnectionSchema):
    TARGET_CLS = BillingAnalyticsCHConnection
