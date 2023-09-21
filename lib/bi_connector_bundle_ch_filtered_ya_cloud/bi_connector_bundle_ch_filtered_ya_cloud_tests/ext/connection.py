from __future__ import annotations

from typing import (
    ClassVar,
    Generic,
    TypeVar,
)

from dl_constants.enums import SourceBackendType
from dl_core.us_connection_base import DataSourceTemplate

from bi_connector_bundle_ch_filtered.base.core.us_connection import ConnectionCHFilteredHardcodedDataBase
from bi_connector_bundle_ch_filtered.testing.connection import CHFilteredConnectionTestClass
from bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.config import (
    EXT_BLACKBOX_USER_UID,
    SR_CONNECTION_DB_NAME,
    SR_CONNECTION_TABLE_NAME,
)


_CONN_TV = TypeVar("_CONN_TV", bound=ConnectionCHFilteredHardcodedDataBase)


class BaseClickhouseFilteredSubselectByPuidConnectionTestClass(
    CHFilteredConnectionTestClass[_CONN_TV], Generic[_CONN_TV]
):
    source_type: ClassVar[SourceBackendType]

    def check_data_source_templates(self, conn: _CONN_TV, dsrc_templates: list[DataSourceTemplate]) -> None:
        expected_template = DataSourceTemplate(
            title=SR_CONNECTION_TABLE_NAME,
            group=[SR_CONNECTION_DB_NAME],
            connection_id=conn.uuid,
            source_type=self.source_type,
            parameters=dict(db_name=SR_CONNECTION_DB_NAME, table_name=SR_CONNECTION_TABLE_NAME),
        )
        assert [expected_template] == dsrc_templates

    def check_saved_connection(self, conn: _CONN_TV, params: dict) -> None:
        assert conn.passport_user_id == EXT_BLACKBOX_USER_UID
        super().check_saved_connection(conn, params)


class ClickhouseFilteredSubselectByPuidConnectionTestWithWrongAuth:
    def test_make_connection(self, saved_connection: _CONN_TV) -> None:
        assert saved_connection.passport_user_id is None
