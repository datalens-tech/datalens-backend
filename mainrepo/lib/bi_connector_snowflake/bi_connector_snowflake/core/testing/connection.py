import uuid
from datetime import datetime
from typing import Optional

from bi_core.us_manager.us_manager_sync import SyncUSManager

from bi_connector_snowflake.core.constants import CONNECTION_TYPE_SNOWFLAKE
from bi_connector_snowflake.core.us_connection import ConnectionSQLSnowFlake
from bi_constants.enums import RawSQLLevel


def make_snowflake_saved_connection(
    sync_usm: SyncUSManager,
    account_name: str,
    user_name: str,
    client_id: str,
    client_secret: str,
    refresh_token: str,
    refresh_token_expire_time: Optional[datetime],
    schema: Optional[str] = None,
    db_name: Optional[str] = None,
    warehouse: Optional[str] = None,
    user_role: Optional[str] = None,
    raw_sql_level: RawSQLLevel = RawSQLLevel.off,
) -> ConnectionSQLSnowFlake:
    conn_name = f"snowflake test conn: {uuid.uuid4()}"
    conn = ConnectionSQLSnowFlake.create_from_dict(
        data_dict=ConnectionSQLSnowFlake.DataModel(
            account_name=account_name,
            user_name=user_name,
            user_role=user_role,
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
            refresh_token_expire_time=refresh_token_expire_time,
            schema=schema,
            db_name=db_name,
            warehouse=warehouse,
            raw_sql_level=raw_sql_level,
        ),
        ds_key=conn_name,
        type_=CONNECTION_TYPE_SNOWFLAKE.name,
        us_manager=sync_usm,
    )
    sync_usm.save(conn)
    return conn
