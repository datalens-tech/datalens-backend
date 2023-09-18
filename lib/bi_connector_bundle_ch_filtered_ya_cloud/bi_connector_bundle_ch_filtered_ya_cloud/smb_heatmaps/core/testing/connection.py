from __future__ import annotations

from typing import Any
import uuid

from dl_constants.enums import RawSQLLevel
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core_testing.connection import make_conn_key

from bi_connector_bundle_ch_filtered_ya_cloud.smb_heatmaps.core.constants import CONNECTION_TYPE_SMB_HEATMAPS
from bi_connector_bundle_ch_filtered_ya_cloud.smb_heatmaps.core.us_connection import ConnectionClickhouseSMBHeatmaps


def make_saved_smb_heatmaps_connection(
    sync_usm: SyncUSManager,
    endpoint: str,
    cluster_name: str,
    max_execution_time: int,
    token: str,
    secure: bool = False,
    raw_sql_level: RawSQLLevel = RawSQLLevel.off,
    **kwargs: Any,
) -> ConnectionClickhouseSMBHeatmaps:
    conn_name = "smb_heatmaps test conn {}".format(uuid.uuid4())
    conn = ConnectionClickhouseSMBHeatmaps.create_from_dict(
        ConnectionClickhouseSMBHeatmaps.DataModel(
            endpoint=endpoint,
            cluster_name=cluster_name,
            max_execution_time=max_execution_time,
            secure=secure,
            token=token,
            raw_sql_level=raw_sql_level,
        ),
        ds_key=make_conn_key("connections", conn_name),
        type_=CONNECTION_TYPE_SMB_HEATMAPS.name,
        meta={"title": conn_name, "state": "saved"},
        us_manager=sync_usm,
        **kwargs,
    )
    sync_usm.save(conn)
    return conn
