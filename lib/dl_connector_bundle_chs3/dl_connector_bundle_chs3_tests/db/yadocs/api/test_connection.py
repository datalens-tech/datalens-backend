from http import HTTPStatus
import logging
import uuid

from flask.testing import FlaskClient
import pytest

from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_testing.utils import get_log_record

from dl_connector_bundle_chs3.chs3_yadocs.core.us_connection import YaDocsFileS3Connection
from dl_connector_bundle_chs3_tests.db.base.api.connection import CHS3ConnectionTestSuite
from dl_connector_bundle_chs3_tests.db.yadocs.api.base import YaDocsFileS3ApiConnectionTestBase


class TestYaDocsFileS3Connection(YaDocsFileS3ApiConnectionTestBase, CHS3ConnectionTestSuite[YaDocsFileS3Connection]):
    @pytest.fixture(scope="function")
    def single_new_conn_source_params(self) -> dict:
        return {
            "id": str(uuid.uuid4()),
            "file_id": str(uuid.uuid4()),
            "title": f"New File {str(uuid.uuid4())}",
        }

    def test_authorization_field(
        self,
        client: FlaskClient,
        sync_us_manager: SyncUSManager,
        saved_connection_id: str,
    ) -> None:
        conn_id = saved_connection_id
        conn = sync_us_manager.get_by_id(conn_id, YaDocsFileS3Connection)
        base_update_data = {
            "refresh_enabled": True,
            "sources": [{"id": src.id, "title": src.title} for src in conn.data.sources],
        }

        # no token => not authorized
        assert conn.authorized is False
        conn_resp = client.get(f"/api/v1/connections/{conn_id}")
        assert conn_resp.status_code == HTTPStatus.OK, conn_resp.json
        assert conn_resp.json["authorized"] is False
        assert "oauth_token" not in conn_resp.json

        # add token into connection
        resp = client.put(
            "/api/v1/connections/{}".format(conn_id),
            json={
                **base_update_data,
                "oauth_token": "some_token",
            },
        )
        assert resp.status_code == HTTPStatus.OK, resp.json
        conn = sync_us_manager.get_by_id(conn_id, YaDocsFileS3Connection)
        assert conn.authorized is True
        conn_resp = client.get(f"/api/v1/connections/{conn_id}")
        assert conn_resp.status_code == HTTPStatus.OK, conn_resp.json
        assert conn_resp.json["authorized"] is True
        assert "oauth_token" not in conn_resp.json

        # remove token from the connection
        resp = client.put(
            "/api/v1/connections/{}".format(conn_id),
            json={
                **base_update_data,
                "oauth_token": None,
            },
        )
        assert resp.status_code == HTTPStatus.OK, resp.json
        conn: YaDocsFileS3Connection = sync_us_manager.get_by_id(conn_id, YaDocsFileS3Connection)
        assert conn.authorized is False
        conn_resp = client.get(f"/api/v1/connections/{conn_id}")
        assert conn_resp.status_code == HTTPStatus.OK
        assert conn_resp.json["authorized"] is False
        assert "oauth_token" not in conn_resp.json

    def test_force_update_with_file_id(
        self,
        caplog,
        client: FlaskClient,
        sync_us_manager: SyncUSManager,
        saved_connection_id: str,
    ) -> None:
        """Passed file_id to an existing source means that it has been updated => this should trigger data update"""

        caplog.set_level(logging.INFO)

        conn_id = saved_connection_id
        usm = sync_us_manager
        conn = usm.get_by_id(conn_id, YaDocsFileS3Connection)

        resp = client.put(
            "/api/v1/connections/{}".format(conn_id),
            json={
                "sources": [
                    {
                        "file_id": str(uuid.uuid4()),  # force source update by passing file_id
                        "id": conn.data.sources[0].id,
                        "title": conn.data.sources[0].title,
                    },
                ],
            },
        )
        assert resp.status_code == HTTPStatus.OK, resp.json

        schedule_save_src_log_record = get_log_record(
            caplog,
            predicate=lambda r: r.message.startswith("Scheduled task SaveSourceTask for source_id"),
            single=True,
        )
        assert conn.data.sources[0].id in schedule_save_src_log_record.message
