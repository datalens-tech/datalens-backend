from __future__ import annotations

from typing import Any
import uuid

from dl_constants.enums import RawSQLLevel
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core_testing.connection import make_conn_key

from bi_connector_bundle_ch_filtered_ya_cloud.market_couriers.core.constants import CONNECTION_TYPE_MARKET_COURIERS
from bi_connector_bundle_ch_filtered_ya_cloud.market_couriers.core.us_connection import (
    ConnectionClickhouseMarketCouriers,
)


def make_saved_market_couriers_connection(
    sync_usm: SyncUSManager,
    endpoint: str,
    cluster_name: str,
    max_execution_time: int,
    token: str,
    secure: bool = False,
    raw_sql_level: RawSQLLevel = RawSQLLevel.off,
    **kwargs: Any,
) -> ConnectionClickhouseMarketCouriers:
    conn_name = "market_couriers test conn {}".format(uuid.uuid4())
    conn = ConnectionClickhouseMarketCouriers.create_from_dict(
        ConnectionClickhouseMarketCouriers.DataModel(
            endpoint=endpoint,
            cluster_name=cluster_name,
            max_execution_time=max_execution_time,
            secure=secure,
            token=token,
            raw_sql_level=raw_sql_level,
        ),
        ds_key=make_conn_key("connections", conn_name),
        type_=CONNECTION_TYPE_MARKET_COURIERS.name,
        meta={"title": conn_name, "state": "saved"},
        us_manager=sync_usm,
        **kwargs,
    )
    sync_usm.save(conn)
    return conn
