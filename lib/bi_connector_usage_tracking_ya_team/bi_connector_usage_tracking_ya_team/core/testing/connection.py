from __future__ import annotations

import uuid
from typing import Any

from bi_constants.enums import RawSQLLevel

from bi_core.us_manager.us_manager_sync import SyncUSManager
from bi_core_testing.connection import make_conn_key

from bi_connector_usage_tracking_ya_team.core.constants import (
    CONNECTION_TYPE_USAGE_TRACKING_YA_TEAM,
)
from bi_connector_usage_tracking_ya_team.core.us_connection import UsageTrackingYaTeamConnection


def make_saved_usage_tracking_ya_team_connection(
    sync_usm: SyncUSManager,
    db_name: str,
    endpoint: str,
    cluster_name: str,
    max_execution_time: int,
    secure: bool = False,
    raw_sql_level: RawSQLLevel = RawSQLLevel.off,
    **kwargs: Any,
) -> UsageTrackingYaTeamConnection:
    conn_name = 'usage_tracking_ya_team test conn {}'.format(uuid.uuid4())
    conn = UsageTrackingYaTeamConnection.create_from_dict(
        UsageTrackingYaTeamConnection.DataModel(
            db_name=db_name,
            endpoint=endpoint,
            cluster_name=cluster_name,
            max_execution_time=max_execution_time,
            secure=secure,
            raw_sql_level=raw_sql_level,
        ),
        ds_key=make_conn_key('connections', conn_name),
        type_=CONNECTION_TYPE_USAGE_TRACKING_YA_TEAM.name,
        meta={'title': conn_name, 'state': 'saved'},
        us_manager=sync_usm,
        **kwargs
    )
    sync_usm.save(conn)
    return conn
