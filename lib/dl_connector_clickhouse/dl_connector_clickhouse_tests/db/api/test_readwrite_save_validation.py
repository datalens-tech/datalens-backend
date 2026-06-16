import json
from typing import ClassVar
import uuid

import pytest

from dl_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase
from dl_constants import RawSQLLevel
from dl_core.connectors.settings.base import ConnectorSettings

from dl_connector_clickhouse.core.clickhouse.settings import ClickHouseConnectorSettings
from dl_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE
from dl_connector_clickhouse_tests.db.api.base import ClickHouseConnectionTestBase


class TestReadwriteAcceptedWhenDirectSQLEnabled(ClickHouseConnectionTestBase):
    """raw_sql_level=readwrite saves successfully when ENABLE_DIRECTSQL is on."""

    raw_sql_level: ClassVar[RawSQLLevel] = RawSQLLevel.readwrite

    @pytest.fixture(scope="class")
    def connectors_settings(self) -> dict[str, ConnectorSettings]:
        return {CONNECTION_TYPE_CLICKHOUSE.value: ClickHouseConnectorSettings(ENABLE_DIRECTSQL=True)}

    def test_readwrite_connection_saved(self, saved_connection_id: str) -> None:
        # `saved_connection_id` performs the POST and asserts HTTP 200 internally.
        assert saved_connection_id


class TestReadwriteRejectedWhenDirectSQLDisabled(ClickHouseConnectionTestBase):
    """POSTing raw_sql_level=readwrite is rejected (400) when ENABLE_DIRECTSQL is off (default)."""

    raw_sql_level: ClassVar[RawSQLLevel] = RawSQLLevel.readwrite
    # no `connectors_settings` override -> default empty -> ENABLE_DIRECTSQL is False

    def test_readwrite_save_rejected(
        self,
        control_api_sync_client: SyncHttpClientBase,
        enriched_connection_params: dict,
        bi_headers: dict[str, str] | None,
    ) -> None:
        data = dict(
            name=f"clickhouse readwrite reject {uuid.uuid4()}",
            type=self.conn_type.name,
            **enriched_connection_params,  # already contains raw_sql_level="readwrite"
        )
        resp = control_api_sync_client.post(
            "/api/v1/connections",
            content_type="application/json",
            data=json.dumps(data),
            headers=bi_headers,
        )
        assert resp.status_code == 400, resp.json
