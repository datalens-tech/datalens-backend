import uuid
from typing import Any, Optional

from dl_constants.enums import RawSQLLevel

from dl_core.us_manager.us_manager_sync import SyncUSManager

from bi_connector_mssql.core.constants import CONNECTION_TYPE_MSSQL
from bi_connector_mssql.core.us_connection import ConnectionMSSQL


def make_mssql_saved_connection(
        sync_usm: SyncUSManager,
        db_name: Optional[str],
        host: str,
        port: Optional[int],
        username: Optional[str],
        password: Optional[str],
        raw_sql_level: RawSQLLevel = RawSQLLevel.off,
        **kwargs: Any,
) -> ConnectionMSSQL:
    conn_name = 'mssql test conn {}'.format(uuid.uuid4())
    conn = ConnectionMSSQL.create_from_dict(
        data_dict=ConnectionMSSQL.DataModel(
            db_name=db_name,
            host=host,
            port=port,
            username=username,
            password=password,
            raw_sql_level=raw_sql_level,
        ),
        ds_key=conn_name,
        type_=CONNECTION_TYPE_MSSQL.name,
        us_manager=sync_usm,
        **kwargs,
    )
    sync_usm.save(conn)
    return conn
