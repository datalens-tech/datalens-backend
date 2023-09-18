from __future__ import annotations

from typing import (
    Any,
    Optional,
)
import uuid

from dl_constants.enums import RawSQLLevel
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core_testing.connection import make_conn_key

from bi_connector_yql.core.yq.constants import CONNECTION_TYPE_YQ
from bi_connector_yql.core.yq.us_connection import YQConnection


def make_saved_yq_connection(
    sync_usm: SyncUSManager,
    service_account_id: Optional[str],
    folder_id: Optional[str],
    password: str,
    raw_sql_level: RawSQLLevel = RawSQLLevel.off,
    **kwargs: Any,
) -> YQConnection:
    conn_name = "yq test conn {}".format(uuid.uuid4())
    conn = YQConnection.create_from_dict(
        YQConnection.DataModel(
            service_account_id=service_account_id,
            folder_id=folder_id,
            password=password,
            raw_sql_level=raw_sql_level,
        ),
        ds_key=make_conn_key("connections", conn_name),
        type_=CONNECTION_TYPE_YQ.name,
        meta={"title": conn_name, "state": "saved"},
        us_manager=sync_usm,
        **kwargs,
    )
    sync_usm.save(conn)
    return conn
