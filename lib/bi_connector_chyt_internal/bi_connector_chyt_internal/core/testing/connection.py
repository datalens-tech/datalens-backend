from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Optional, Type

from bi_constants.enums import ConnectionType, RawSQLLevel

from bi_core.us_manager.us_manager import USManagerBase
from bi_core.us_manager.us_manager_sync import SyncUSManager
from bi_connector_chyt_internal.core.us_connection import ConnectionCHYTInternalToken, ConnectionCHYTUserAuth
from bi_core_testing.connection import make_connection_base

if TYPE_CHECKING:
    from bi_core.us_connection_base import ConnectionBase


def make_chyt_connection(
        sync_usm: USManagerBase,
        conn_type: ConnectionType = ConnectionType.ch_over_yt,
        conn_cls: Type[ConnectionBase] = ConnectionCHYTInternalToken,
        alias: Optional[str] = "*chyt_datalens_back",
        cluster: str = "hahn",
        raw_sql_level: RawSQLLevel = RawSQLLevel.dashsql,
        token: Optional[str] = None,
) -> ConnectionBase:
    data_dict = dict(
        alias=alias,
        cluster=cluster,
        raw_sql_level=raw_sql_level,
    )
    if token is not None:
        data_dict.update(token=token)
    return make_connection_base(
        us_manager=sync_usm,
        conn_type=conn_type,
        conn_name=f"chyt conn {uuid.uuid4()}",
        data_dict=data_dict,
        conn_cls=conn_cls,
    )


def make_saved_chyt_connection(
        sync_usm: SyncUSManager,
        alias: Optional[str] = "*chyt_datalens_back",
        cluster: str = "hahn",
        raw_sql_level: RawSQLLevel = RawSQLLevel.dashsql,
        token: str = 'dummy_token',
) -> ConnectionCHYTInternalToken:
    conn = make_chyt_connection(sync_usm, alias=alias, cluster=cluster, raw_sql_level=raw_sql_level, token=token)
    assert isinstance(conn, ConnectionCHYTInternalToken)
    sync_usm.save(conn)
    return conn


def make_chyt_user_auth_connection(
        sync_usm: SyncUSManager,
        alias: Optional[str] = "*chyt_datalens_back",
        cluster: str = "hahn",
        raw_sql_level: RawSQLLevel = RawSQLLevel.dashsql,
        token: Optional[str] = None,
) -> ConnectionCHYTUserAuth:
    conn = make_chyt_connection(
        sync_usm,
        conn_type=ConnectionType.ch_over_yt_user_auth,
        conn_cls=ConnectionCHYTUserAuth,
        alias=alias,
        cluster=cluster,
        raw_sql_level=raw_sql_level,
        token=token,
    )
    assert isinstance(conn, ConnectionCHYTUserAuth)
    return conn


def make_saved_chyt_user_auth_connection(
        sync_usm: SyncUSManager,
        alias: Optional[str] = "*chyt_datalens_back",
        cluster: str = "hahn",
        raw_sql_level: RawSQLLevel = RawSQLLevel.dashsql,
) -> ConnectionCHYTUserAuth:
    conn = make_chyt_user_auth_connection(
        sync_usm,
        alias=alias,
        cluster=cluster,
        raw_sql_level=raw_sql_level,
    )
    sync_usm.save(conn)
    return conn
