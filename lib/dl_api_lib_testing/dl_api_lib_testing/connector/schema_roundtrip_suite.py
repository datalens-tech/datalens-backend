import json
from typing import Any
from unittest import mock
import uuid

import pytest

from dl_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase
from dl_api_lib_testing.connection_base import ConnectionTestBase


class ConnectionSchemaRoundtripTestSuite(ConnectionTestBase):
    """Roundtrip schema test: POST a fully-populated connection, GET it back, assert equality.

    Subclasses must mix this in with a connector test base (e.g. `ClickHouseConnectionTestBase`)
    and provide two fixtures:

        - `input_data`:  the POST payload (every field the API schema accepts).
        - `output_data`: the expected GET response. May depend on `input_data`.

    Use `unittest.mock.ANY` in `output_data` for server-generated values such as `id`,
    `created_at`, `updated_at`. The test asserts `response == output_data`, and `mock.ANY`
    matches any value under Python equality.

    Spread ``self._common_output_data(...)`` into ``output_data`` to fill in the
    server-set fields (``id``, ``key``, ``created_at``, ``updated_at``,
    ``description``) and the ``options`` block, which is structurally identical
    across connectors and varies only in four boolean flags.

    For connectors with mutually exclusive field sets (e.g. password vs key auth),
    parameterize `input_data` and `output_data` with `pytest.fixture(params=...)`.
    """

    @pytest.fixture(scope="class")
    def input_data(self) -> dict[str, Any]:
        raise NotImplementedError("Subclass must provide `input_data`")

    @pytest.fixture(scope="class")
    def output_data(self, input_data: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError("Subclass must provide `output_data`")

    def _common_output_data(
        self,
        *,
        db_type: str,
        allow_dashsql_usage: bool = False,
        allow_dataset_usage: bool = True,
        allow_typed_query_usage: bool = False,
        allow_pagination_usage: bool = False,
        allow_selector: bool = False,
    ) -> dict[str, Any]:
        return {
            "id": mock.ANY,
            "key": mock.ANY,
            "created_at": mock.ANY,
            "updated_at": mock.ANY,
            "db_type": db_type,
            "description": "",
            "meta": {},
            "options": {
                "allow_dashsql_usage": allow_dashsql_usage,
                "allow_dataset_usage": allow_dataset_usage,
                "allow_typed_query_usage": allow_typed_query_usage,
                "allow_pagination_usage": allow_pagination_usage,
                "query_types": [
                    {
                        "allow_selector": allow_selector,
                        "query_type": "generic_query",
                        "query_type_label": "Generic Query",
                        "required_parameters": [],
                    },
                ],
            },
        }

    def test_schema_roundtrip(
        self,
        control_api_sync_client: SyncHttpClientBase,
        bi_headers: dict[str, str] | None,
        input_data: dict[str, Any],
        output_data: dict[str, Any],
    ) -> None:
        post_payload = dict(
            name=f"{self.conn_type.name} schema-roundtrip {uuid.uuid4()}",
            type=self.conn_type.name,
            **input_data,
        )
        post_resp = control_api_sync_client.post(
            "/api/v1/connections",
            content_type="application/json",
            data=json.dumps(post_payload),
            headers=bi_headers,
        )
        assert post_resp.status_code == 200, post_resp.json
        conn_id = post_resp.json["id"]

        try:
            get_resp = control_api_sync_client.get(
                f"/api/v1/connections/{conn_id}",
                headers=bi_headers,
            )
            assert get_resp.status_code == 200, get_resp.json
            assert get_resp.json == output_data, get_resp.json
        finally:
            control_api_sync_client.delete(f"/api/v1/connections/{conn_id}")
