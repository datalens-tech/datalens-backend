from __future__ import annotations

from typing import ClassVar, Generic, TypeVar

from dl_core.us_connection_base import ConnectionSQL

from dl_testing.regulated_test import RegulatedTestParams
from dl_core_testing.testcases.connection import DefaultConnectionTestClass

from bi_connector_bundle_ch_filtered.base.core.settings import ServiceConnectorSettingsBase

_CONN_TV = TypeVar('_CONN_TV', bound=ConnectionSQL)


class CHFilteredConnectionTestClass(DefaultConnectionTestClass[_CONN_TV], Generic[_CONN_TV]):
    sr_connection_settings: ClassVar[ServiceConnectorSettingsBase]

    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultConnectionTestClass.test_connection_get_data_source_templates: '',  # TODO: FIXME
        },
    )

    def check_saved_connection(self, conn: _CONN_TV, params: dict) -> None:
        assert conn.uuid is not None
        data_fields = (
            'endpoint', 'cluster_name', 'max_execution_time',
        )
        for f_name in data_fields:
            assert getattr(conn.data, f_name) == params[f_name]

        hardcoded_dto_fields = (
            'host', 'port', 'db_name', 'username', 'password',
        )
        conn_dto = conn.get_conn_dto()
        assert conn._us_resp
        us_resp = conn._us_resp.get('data')
        assert us_resp is not None
        for f_name in hardcoded_dto_fields:
            assert us_resp.get(f_name) is None
            assert getattr(conn.data, f_name) is None
            assert getattr(conn_dto, f_name) == getattr(self.sr_connection_settings, f_name.upper())

        hardcoded_properties = (
            'allowed_tables', 'allow_public_usage',
        )
        for f_name in hardcoded_properties:
            assert getattr(conn, f_name) is not None
