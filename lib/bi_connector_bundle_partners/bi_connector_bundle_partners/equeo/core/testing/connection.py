from __future__ import annotations

from typing import Any
import uuid

from dl_constants.enums import RawSQLLevel
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core_testing.connection import make_conn_key

from bi_connector_bundle_partners.equeo.core.constants import CONNECTION_TYPE_EQUEO
from bi_connector_bundle_partners.equeo.core.us_connection import EqueoCHConnection


def make_saved_equeo_connection(
    sync_usm: SyncUSManager,
    db_name: str,
    endpoint: str,
    cluster_name: str,
    max_execution_time: int,
    secure: bool = False,
    raw_sql_level: RawSQLLevel = RawSQLLevel.off,
    **kwargs: Any,
) -> EqueoCHConnection:
    conn_name = f"equeo test conn {uuid.uuid4()}"
    conn = EqueoCHConnection.create_from_dict(
        EqueoCHConnection.DataModel(
            db_name=db_name,
            endpoint=endpoint,
            cluster_name=cluster_name,
            max_execution_time=max_execution_time,
            secure=secure,
            raw_sql_level=raw_sql_level,
        ),
        ds_key=make_conn_key("connection", conn_name),
        type_=CONNECTION_TYPE_EQUEO.name,
        meta={"title": conn_name, "state": "saved"},
        us_manager=sync_usm,
        **kwargs,
    )
    sync_usm.save(conn)
    return conn
