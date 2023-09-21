from __future__ import annotations

from typing import (
    ClassVar,
    Generic,
    TypeVar,
)

from dl_constants.enums import SourceBackendType
from dl_core.us_connection_base import DataSourceTemplate
from dl_core_testing.testcases.connection import DefaultConnectionTestClass

from bi_connector_bundle_ch_filtered.base.core.settings import ServiceConnectorSettingsBase
from bi_connector_bundle_partners.base.core.us_connection import PartnersCHConnectionBase
import bi_connector_bundle_partners_tests.db.config as test_config


_CONN_TV = TypeVar("_CONN_TV", bound=PartnersCHConnectionBase)


class PartnersConnectionTestClass(DefaultConnectionTestClass[_CONN_TV], Generic[_CONN_TV]):
    source_type: ClassVar[SourceBackendType]
    sr_connection_settings: ClassVar[ServiceConnectorSettingsBase]

    def check_data_source_templates(self, conn: _CONN_TV, dsrc_templates: list[DataSourceTemplate]) -> None:
        expected_template = DataSourceTemplate(
            title=test_config.TABLE_NAME,
            group=[],
            connection_id=conn.uuid,
            source_type=self.source_type,
            parameters=dict(db_name=None, table_name=test_config.TABLE_NAME),
        )
        assert [expected_template] == dsrc_templates

    def check_saved_connection(self, conn: _CONN_TV, params: dict) -> None:
        hardcoded_dto_fields = (
            "host",
            "port",
            "username",
            "password",
        )
        conn_dto = conn.get_conn_dto()
        assert conn._us_resp
        us_resp = conn._us_resp.get("data")
        assert us_resp is not None
        for f_name in hardcoded_dto_fields:
            assert us_resp.get(f_name) is None
            assert getattr(conn.data, f_name) is None
            assert getattr(conn_dto, f_name) == getattr(self.sr_connection_settings, f_name.upper())

        hardcoded_properties = ("allow_public_usage",)
        for f_name in hardcoded_properties:
            assert getattr(conn, f_name) is not None

        assert conn.data.db_name == test_config.DB_NAME
        assert conn_dto.db_name == test_config.DB_NAME
