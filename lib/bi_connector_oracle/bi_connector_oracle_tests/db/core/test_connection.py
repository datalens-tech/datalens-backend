from bi_core.us_connection_base import DataSourceTemplate

from bi_testing.regulated_test import RegulatedTestParams
from bi_core_testing.testcases.connection import DefaultConnectionTestClass

from bi_connector_oracle.core.us_connection import ConnectionSQLOracle
from bi_connector_oracle_tests.db.core.base import BaseOracleTestClass
from bi_connector_oracle_tests.db.config import DEFAULT_ORACLE_SCHEMA_NAME


class TestOracleConnection(
        BaseOracleTestClass,
        DefaultConnectionTestClass[ConnectionSQLOracle],
):
    do_check_data_export_flag = True
    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultConnectionTestClass.test_connection_get_data_source_templates: '',  # TODO: FIXME
        }
    )

    def check_saved_connection(self, conn: ConnectionSQLOracle, params: dict) -> None:
        assert conn.uuid is not None
        assert conn.data.db_name == params['db_name']
        assert conn.data.host == params['host']
        assert conn.data.port == params['port']
        assert conn.data.username == params['username']
        assert conn.data.password == params['password']

    def check_data_source_templates(
            self, conn: ConnectionSQLOracle, dsrc_templates: list[DataSourceTemplate],
    ) -> None:
        assert dsrc_templates
        for dsrc_tmpl in dsrc_templates:
            assert dsrc_tmpl.title
            if dsrc_tmpl.parameters.get('schema_name') is not None:
                assert dsrc_tmpl.group == [dsrc_tmpl.parameters['schema_name']]

        schema_names = {tmpl.parameters.get('schema_name') for tmpl in dsrc_templates}
        assert DEFAULT_ORACLE_SCHEMA_NAME in schema_names
