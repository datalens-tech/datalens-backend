from bi_core.us_connection_base import DataSourceTemplate
from bi_core_testing.testcases.connection import DefaultConnectionTestClass

from bi_connector_bigquery.core.us_connection import ConnectionSQLBigQuery

from bi_connector_bigquery_tests.ext.core.base import BaseBigQueryTestClass


class TestBigQueryConnection(
        BaseBigQueryTestClass,
        DefaultConnectionTestClass[ConnectionSQLBigQuery],
):
    def check_saved_connection(self, conn: ConnectionSQLBigQuery, params: dict) -> None:
        assert conn.uuid is not None
        assert conn.data.project_id == params['project_id']
        assert conn.data.credentials == params['credentials']

    def check_data_source_templates(
            self, conn: ConnectionSQLBigQuery, dsrc_templates: list[DataSourceTemplate],
    ) -> None:
        for dsrc_tmpl in dsrc_templates:
            assert conn.project_id in dsrc_tmpl.group
            assert dsrc_tmpl.title
