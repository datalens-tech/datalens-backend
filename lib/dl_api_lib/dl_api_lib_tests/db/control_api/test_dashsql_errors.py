import json

from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_constants.enums import RawSQLLevel
from dl_core.data_source import BaseSQLDataSource
from dl_core.exc import FailedToLoadSchema


class TestDashSQLControlApiErrors(DefaultApiTestBase):
    raw_sql_level = RawSQLLevel.dashsql

    def test_empty_source_schema_error(self, db, sample_table, saved_connection_id, control_api, monkeypatch):
        def mock_get_schema_info(self, conn_executor_factory):
            raise FailedToLoadSchema()

        monkeypatch.setattr(BaseSQLDataSource, "get_schema_info", mock_get_schema_info)

        resp = control_api.client.post(
            f"/api/v1/connections/{saved_connection_id}/info/source/schema",
            data=json.dumps(
                {
                    "source": {
                        "title": sample_table.name,
                        "source_type": "CH_TABLE",
                        "connection_id": saved_connection_id,
                        "parameters": {
                            "db_name": db.name,
                            "table_name": sample_table.name,
                        },
                        "id": "sample",
                    },
                }
            ),
            content_type="application/json",
        )

        assert resp.status_code == 400
        assert resp.json["message"] == "Failed to load description of table columns."
        assert resp.json["code"] == "ERR.DS_API.COLUMN_SCHEMA_FAILED"
