from typing import (
    Any,
    Optional,
)
import uuid

from dl_constants.enums import RawSQLLevel
from dl_core.us_manager.us_manager_sync import SyncUSManager

from dl_connector_oracle.core.constants import CONNECTION_TYPE_ORACLE
from dl_connector_oracle.core.us_connection import ConnectionSQLOracle


def make_oracle_saved_connection(
    sync_usm: SyncUSManager,
    db_name: Optional[str],
    host: str,
    port: Optional[int],
    username: Optional[str],
    password: Optional[str],
    raw_sql_level: RawSQLLevel = RawSQLLevel.off,
    **kwargs: Any,
) -> ConnectionSQLOracle:
    conn_name = "oracle test conn {}".format(uuid.uuid4())
    conn = ConnectionSQLOracle.create_from_dict(
        data_dict=ConnectionSQLOracle.DataModel(
            db_name=db_name,
            host=host,
            port=port,
            username=username,
            password=password,
            raw_sql_level=raw_sql_level,
        ),
        ds_key=conn_name,
        type_=CONNECTION_TYPE_ORACLE.name,
        us_manager=sync_usm,
        **kwargs,
    )
    sync_usm.save(conn)
    return conn
