from typing import (
    Any,
    Optional,
)
import uuid

from dl_connector_postgresql.core.postgresql.constants import CONNECTION_TYPE_POSTGRES
from dl_connector_postgresql.core.postgresql.us_connection import ConnectionPostgreSQL
from dl_constants.enums import RawSQLLevel
from dl_core.us_manager.us_manager_sync import SyncUSManager


def make_postgresql_saved_connection(
    sync_usm: SyncUSManager,
    db_name: Optional[str],
    host: str,
    port: Optional[int],
    username: Optional[str],
    password: Optional[str],
    raw_sql_level: RawSQLLevel = RawSQLLevel.off,
    ssl_enable: bool = False,
    ssl_ca: Optional[str] = None,
    **kwargs: Any,
) -> ConnectionPostgreSQL:
    conn_name = "postgres test conn {}".format(uuid.uuid4())
    conn = ConnectionPostgreSQL.create_from_dict(
        data_dict=ConnectionPostgreSQL.DataModel(
            db_name=db_name,
            host=host,
            port=port,
            username=username,
            password=password,
            raw_sql_level=raw_sql_level,
            ssl_enable=ssl_enable,
            ssl_ca=ssl_ca,
        ),
        ds_key=conn_name,
        type_=CONNECTION_TYPE_POSTGRES.name,
        us_manager=sync_usm,
        **kwargs,
    )
    sync_usm.save(conn)
    return conn
