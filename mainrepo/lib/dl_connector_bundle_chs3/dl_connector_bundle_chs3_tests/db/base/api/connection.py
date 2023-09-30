import abc
from http import HTTPStatus
import uuid

from flask.testing import FlaskClient
import pytest

from dl_api_lib_testing.connector.connection_suite import DefaultConnectorConnectionTestSuite
from dl_connector_bundle_chs3.chs3_base.core.us_connection import BaseFileS3Connection
from dl_connector_bundle_chs3_tests.db.base.api.base import CHS3ConnectionApiTestBase
from dl_connector_bundle_chs3_tests.db.base.core.base import FILE_CONN_TV
from dl_core.exc import DataSourceTitleConflict
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_testing.regulated_test import RegulatedTestParams


class CHS3ConnectionTestSuite(
    CHS3ConnectionApiTestBase[FILE_CONN_TV],
    DefaultConnectorConnectionTestSuite,
    metaclass=abc.ABCMeta,
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultConnectorConnectionTestSuite.test_test_connection: "Not implemented",
            DefaultConnectorConnectionTestSuite.test_cache_ttl_sec_override: "Unavailable for CHS3 connectors",
        },
    )

    @abc.abstractmethod
    @pytest.fixture(scope="function")
    def single_new_conn_source_params(self) -> dict:
        raise NotImplementedError()

    def test_add_and_drop_connection_source(
        self,
        client: FlaskClient,
        sync_us_manager: SyncUSManager,
        saved_connection_id: str,
        single_new_conn_source_params: dict,
    ) -> None:
        conn_id = saved_connection_id
        orig_conn = sync_us_manager.get_by_id(conn_id, BaseFileS3Connection)

        new_source = single_new_conn_source_params
        add_resp = client.put(
            "/api/v1/connections/{}".format(conn_id),
            json={
                "sources": [
                    {"id": orig_conn.data.sources[0].id, "title": orig_conn.data.sources[0].title},
                    new_source,
                ],
            },
        )
        assert add_resp.status_code == HTTPStatus.OK, add_resp.json

        conn = sync_us_manager.get_by_id(conn_id)
        assert len(conn.data.sources) == len(orig_conn.data.sources) + 1

        drop_resp = client.put(
            "/api/v1/connections/{}".format(conn_id),
            json={
                "sources": [
                    {"id": orig_conn.data.sources[0].id, "title": orig_conn.data.sources[0].title},
                ],
            },
        )
        assert drop_resp.status_code == HTTPStatus.OK, drop_resp.json

        conn = sync_us_manager.get_by_id(conn_id)
        assert len(conn.data.sources) == len(orig_conn.data.sources)

    def test_rename_connection_source(
        self,
        client: FlaskClient,
        sync_us_manager: SyncUSManager,
        saved_connection_id: str,
    ) -> None:
        conn_id = saved_connection_id
        orig_conn = sync_us_manager.get_by_id(conn_id, BaseFileS3Connection)

        resp = client.put(
            "/api/v1/connections/{}".format(conn_id),
            json={
                "sources": [
                    {"id": orig_conn.data.sources[0].id, "title": "renamed source"},
                ],
            },
        )
        assert resp.status_code == HTTPStatus.OK, resp.json

        conn = sync_us_manager.get_by_id(conn_id)
        assert conn.data.sources[0].title == "renamed source"

    def test_replace_connection_source(
        self,
        client: FlaskClient,
        sync_us_manager: SyncUSManager,
        saved_connection_id: str,
        single_new_conn_source_params: dict,
    ) -> None:
        conn_id = saved_connection_id
        orig_conn = sync_us_manager.get_by_id(conn_id, BaseFileS3Connection)

        source_to_replace = orig_conn.data.sources[0]
        new_source = single_new_conn_source_params
        resp = client.put(
            "/api/v1/connections/{}".format(conn_id),
            json={
                "sources": [new_source],
                "replace_sources": [
                    {
                        "old_source_id": source_to_replace.id,
                        "new_source_id": new_source["id"],
                    },
                ],
            },
        )
        assert resp.status_code == HTTPStatus.OK, resp.json
        conn = sync_us_manager.get_by_id(conn_id, BaseFileS3Connection)

        old_source_ids = set(src.id for src in orig_conn.data.sources)
        new_source_ids = set(src.id for src in conn.data.sources)
        assert old_source_ids == new_source_ids
        new_replaced_source = conn.get_file_source_by_id(source_to_replace.id)
        assert new_replaced_source.file_id != source_to_replace.file_id

    def test_consistency_checks_pass_not_configured_source(
        self,
        client: FlaskClient,
        sync_us_manager: SyncUSManager,
        saved_connection_id: str,
        single_new_conn_source_params: dict,
    ) -> None:
        conn = sync_us_manager.get_by_id(saved_connection_id, BaseFileS3Connection)

        new_source = single_new_conn_source_params
        new_source.pop("file_id")  # as if this source already exists
        resp = client.put(
            "/api/v1/connections/{}".format(saved_connection_id),
            json={
                "sources": [
                    {"id": conn.data.sources[0].id, "title": conn.data.sources[0].title},
                    new_source,
                ],
            },
        )
        assert resp.status_code == HTTPStatus.BAD_REQUEST, resp.json
        details: dict[str, list[str]] = resp.json["details"]
        assert details == {
            "not_configured_not_saved": [new_source["id"]],
        }

    def test_consistency_checks_replace_non_existent_source(
        self,
        client: FlaskClient,
        sync_us_manager: SyncUSManager,
        saved_connection_id: str,
        single_new_conn_source_params: dict,
    ) -> None:
        conn = sync_us_manager.get_by_id(saved_connection_id, BaseFileS3Connection)

        replaced_source_id = str(uuid.uuid4())  # replacing a non-existent source
        new_source = single_new_conn_source_params
        resp = client.put(
            "/api/v1/connections/{}".format(saved_connection_id),
            json={
                "sources": [
                    {"id": conn.data.sources[0].id, "title": conn.data.sources[0].title},
                    new_source,
                ],
                "replace_sources": [
                    {
                        "old_source_id": replaced_source_id,
                        "new_source_id": new_source["id"],
                    },
                ],
            },
        )
        assert resp.status_code == HTTPStatus.BAD_REQUEST, resp.json
        details: dict[str, list[str]] = resp.json["details"]
        assert details == {
            "replaced_not_saved": [replaced_source_id],
        }

    def test_consistency_checks_non_unique_titles(
        self,
        client: FlaskClient,
        sync_us_manager: SyncUSManager,
        saved_connection_id: str,
        single_new_conn_source_params: dict,
    ) -> None:
        conn = sync_us_manager.get_by_id(saved_connection_id, BaseFileS3Connection)

        new_source = single_new_conn_source_params
        new_source["title"] = conn.data.sources[0].title
        resp = client.put(
            "/api/v1/connections/{}".format(saved_connection_id),
            json={
                "sources": [
                    {"id": conn.data.sources[0].id, "title": conn.data.sources[0].title},
                    new_source,
                ],
            },
        )
        assert resp.status_code == HTTPStatus.BAD_REQUEST, resp.json
        assert resp.json["message"] == DataSourceTitleConflict().message

    def test_table_name_spoofing(
        self,
        client: FlaskClient,
        sync_us_manager: SyncUSManager,
        saved_connection_id: str,
    ) -> None:
        usm = sync_us_manager
        orig_conn = usm.get_by_id(saved_connection_id, BaseFileS3Connection)
        orig_filename = orig_conn.data.sources[0].s3_filename

        fake_filename = "hack_me.native"
        resp = client.put(
            "/api/v1/connections/{}".format(saved_connection_id),
            json={
                "sources": [
                    {
                        "id": orig_conn.data.sources[0].id,
                        "title": orig_conn.data.sources[0].title,
                        "s3_filename": fake_filename,
                    }
                ],
            },
        )
        assert resp.status_code == HTTPStatus.BAD_REQUEST, resp.json
        assert resp.json["sources"]["0"]["s3_filename"] == ["Unknown field."]
        conn = usm.get_by_id(saved_connection_id, BaseFileS3Connection)
        assert conn.data.sources[0].s3_filename == orig_filename
