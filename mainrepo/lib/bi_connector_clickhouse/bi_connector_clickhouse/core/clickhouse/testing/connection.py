from typing import (
    Any,
    Optional,
)
import uuid

from bi_constants.enums import RawSQLLevel
from bi_core.us_manager.us_manager_sync import SyncUSManager

from bi_connector_clickhouse.core.clickhouse.constants import CONNECTION_TYPE_CLICKHOUSE
from bi_connector_clickhouse.core.clickhouse.us_connection import ConnectionClickhouse


def make_clickhouse_saved_connection(
    sync_usm: SyncUSManager,
    db_name: Optional[str],
    host: str,
    port: Optional[int],
    username: Optional[str],
    password: Optional[str],
    raw_sql_level: RawSQLLevel = RawSQLLevel.off,
    secure: bool = False,
    ssl_ca: Optional[str] = None,
    **kwargs: Any,
) -> ConnectionClickhouse:
    conn_name = "ch test conn {}".format(uuid.uuid4())
    conn = ConnectionClickhouse.create_from_dict(
        data_dict=ConnectionClickhouse.DataModel(
            db_name=db_name,
            host=host,
            port=port,
            username=username,
            password=password,
            raw_sql_level=raw_sql_level,
            secure=secure,
            ssl_ca=ssl_ca,
        ),
        ds_key=conn_name,
        type_=CONNECTION_TYPE_CLICKHOUSE.name,
        us_manager=sync_usm,
        **kwargs,
    )
    sync_usm.save(conn)
    return conn
