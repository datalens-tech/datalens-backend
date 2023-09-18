from __future__ import annotations

from typing import Any
import uuid

from dl_constants.enums import RawSQLLevel
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core_testing.connection import make_conn_key

from bi_connector_bundle_ch_filtered.ch_billing_analytics.core.constants import CONNECTION_TYPE_CH_BILLING_ANALYTICS
from bi_connector_bundle_ch_filtered.ch_billing_analytics.core.us_connection import BillingAnalyticsCHConnection


def make_saved_ch_billing_analytics_connection(
    sync_usm: SyncUSManager,
    endpoint: str,
    cluster_name: str,
    max_execution_time: int,
    secure: bool = False,
    raw_sql_level: RawSQLLevel = RawSQLLevel.off,
    **kwargs: Any,
) -> BillingAnalyticsCHConnection:
    conn_name = "ch_billing_analytics test conn {}".format(uuid.uuid4())
    conn = BillingAnalyticsCHConnection.create_from_dict(
        BillingAnalyticsCHConnection.DataModel(
            endpoint=endpoint,
            cluster_name=cluster_name,
            max_execution_time=max_execution_time,
            secure=secure,
            raw_sql_level=raw_sql_level,
        ),
        ds_key=make_conn_key("connections", conn_name),
        type_=CONNECTION_TYPE_CH_BILLING_ANALYTICS.name,
        meta={"title": conn_name, "state": "saved"},
        us_manager=sync_usm,
        **kwargs,
    )
    sync_usm.save(conn)
    return conn
