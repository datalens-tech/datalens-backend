from typing import Generic, TypeVar

import attr

from dl_testing.regulated_test import RegulatedTestParams
from dl_core_testing.testcases.connection import DefaultConnectionTestClass

from bi_connector_chyt_internal.core.us_connection import ConnectionCHYTInternalToken, ConnectionCHYTUserAuth

from bi_connector_chyt_internal_tests.ext.core.base import BaseCHYTTestClass, BaseCHYTUserAuthTestClass

_CONN_TV = TypeVar('_CONN_TV', ConnectionCHYTInternalToken, ConnectionCHYTUserAuth)


class CHYTConnectionTestCase(DefaultConnectionTestClass[_CONN_TV], Generic[_CONN_TV]):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultConnectionTestClass.test_connection_get_data_source_templates: '',  # TODO: FIXME
        },
    )
    do_check_data_export_flag = True

    def check_saved_connection(self, conn: _CONN_TV, params: dict) -> None:
        assert conn.uuid is not None
        conn_data = attr.asdict(conn.data)
        assert params.items() <= conn_data.items()


class TestCHYTConnection(BaseCHYTTestClass, CHYTConnectionTestCase[ConnectionCHYTInternalToken]):
    pass


class TestCHYTUserAuthConnection(BaseCHYTUserAuthTestClass, CHYTConnectionTestCase[ConnectionCHYTUserAuth]):
    pass
