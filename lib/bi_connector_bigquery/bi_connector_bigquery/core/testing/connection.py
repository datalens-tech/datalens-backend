import uuid
from typing import Any

from bi_constants.enums import RawSQLLevel

from bi_core.us_manager.us_manager_sync import SyncUSManager

from bi_connector_bigquery.core.constants import CONNECTION_TYPE_BIGQUERY
from bi_connector_bigquery.core.us_connection import ConnectionSQLBigQuery


def make_bigquery_saved_connection(
        sync_usm: SyncUSManager,
        project_id: str,
        credentials: str,
        raw_sql_level: RawSQLLevel = RawSQLLevel.off,
        **kwargs: Any,
) -> ConnectionSQLBigQuery:
    conn_name = 'bigquery test conn {}'.format(uuid.uuid4())
    conn = ConnectionSQLBigQuery.create_from_dict(
        data_dict=ConnectionSQLBigQuery.DataModel(
            project_id=project_id,
            credentials=credentials,
            raw_sql_level=raw_sql_level,
        ),
        ds_key=conn_name,
        type_=CONNECTION_TYPE_BIGQUERY.name,
        us_manager=sync_usm,
        **kwargs)
    sync_usm.save(conn)
    return conn
