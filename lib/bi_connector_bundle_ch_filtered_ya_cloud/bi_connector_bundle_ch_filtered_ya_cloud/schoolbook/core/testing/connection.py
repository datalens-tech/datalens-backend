from __future__ import annotations

import uuid
from typing import Any

from bi_constants.enums import RawSQLLevel

from bi_core.us_manager.us_manager_sync import SyncUSManager
from bi_connector_bundle_ch_filtered_ya_cloud.schoolbook.core.constants import (
    CONNECTION_TYPE_SCHOOLBOOK_JOURNAL,
)
from bi_connector_bundle_ch_filtered_ya_cloud.schoolbook.core.us_connection import ConnectionClickhouseSchoolbook
from bi_core_testing.connection import make_conn_key


def make_saved_schoolbook_journal_connection(
    sync_usm: SyncUSManager,
    endpoint: str,
    cluster_name: str,
    max_execution_time: int,
    token: str,
    secure: bool = False,
    raw_sql_level: RawSQLLevel = RawSQLLevel.off,
    **kwargs: Any,
) -> ConnectionClickhouseSchoolbook:
    conn_name = 'schoolbook_journal test conn {}'.format(uuid.uuid4())
    conn = ConnectionClickhouseSchoolbook.create_from_dict(
        ConnectionClickhouseSchoolbook.DataModel(
            endpoint=endpoint,
            cluster_name=cluster_name,
            max_execution_time=max_execution_time,
            secure=secure,
            token=token,
            raw_sql_level=raw_sql_level,
        ),
        ds_key=make_conn_key('connections', conn_name),
        type_=CONNECTION_TYPE_SCHOOLBOOK_JOURNAL.name,
        meta={'title': conn_name, 'state': 'saved'},
        us_manager=sync_usm,
        **kwargs
    )
    sync_usm.save(conn)
    return conn
