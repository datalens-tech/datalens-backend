from __future__ import annotations

from typing import Generic, TypeVar

from dl_core.us_connection_base import ConnectionSQL

from bi_connector_bundle_ch_filtered.testing.connection import CHFilteredConnectionTestClass
from bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.config import EXT_BLACKBOX_USER_UID


_CONN_TV = TypeVar('_CONN_TV', bound=ConnectionSQL)


class BaseClickhouseFilteredSubselectByPuidConnectionTestClass(CHFilteredConnectionTestClass[_CONN_TV], Generic[_CONN_TV]):
    def check_saved_connection(self, conn: _CONN_TV, params: dict) -> None:
        assert conn.passport_user_id == EXT_BLACKBOX_USER_UID
        super().check_saved_connection(conn, params)


class ClickhouseFilteredSubselectByPuidConnectionTestWithWrongAuth:
    def test_make_connection(self, saved_connection: _CONN_TV) -> None:
        assert saved_connection.passport_user_id is None
