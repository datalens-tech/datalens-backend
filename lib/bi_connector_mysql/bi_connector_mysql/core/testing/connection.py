import uuid
from typing import Any, Optional

from dl_constants.enums import RawSQLLevel

from dl_core.us_manager.us_manager_sync import SyncUSManager

from bi_connector_mysql.core.constants import CONNECTION_TYPE_MYSQL
from bi_connector_mysql.core.us_connection import ConnectionMySQL


def make_mysql_saved_connection(
        sync_usm: SyncUSManager,
        db_name: Optional[str],
        host: str,
        port: Optional[int],
        username: Optional[str],
        password: Optional[str],
        raw_sql_level: RawSQLLevel = RawSQLLevel.off,
        **kwargs: Any,
) -> ConnectionMySQL:
    conn_name = 'mysql test conn {}'.format(uuid.uuid4())
    conn = ConnectionMySQL.create_from_dict(
        data_dict=ConnectionMySQL.DataModel(
            db_name=db_name,
            host=host,
            port=port,
            username=username,
            password=password,
            raw_sql_level=raw_sql_level,
        ),
        ds_key=conn_name,
        type_=CONNECTION_TYPE_MYSQL.name,
        us_manager=sync_usm,
        **kwargs,
    )
    sync_usm.save(conn)
    return conn
