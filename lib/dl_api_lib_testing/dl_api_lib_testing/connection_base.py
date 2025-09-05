import abc
import contextlib
import json
from typing import (
    Any,
    ClassVar,
    Generator,
)
import uuid

import attr
import pytest

from dl_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase
from dl_api_lib_testing.base import ApiTestBase
from dl_constants.enums import ConnectionType
from dl_core_testing.database import (
    Db,
    DbTable,
)
from dl_core_testing.fixtures.dispenser import DbCsvTableDispenser
from dl_core_testing.fixtures.sample_tables import TABLE_SPEC_SAMPLE_SUPERSTORE
from dl_core_testing.testcases.service_base import DbServiceFixtureTextClass


@attr.s(kw_only=True, frozen=True)
class EditConnectionParamsCase:
    params: dict[str, Any] = attr.ib(factory=dict)
    load_only_field_names: list[str] = attr.ib(factory=list)


class ConnectionTestBase(ApiTestBase, DbServiceFixtureTextClass):
    conn_type: ClassVar[ConnectionType]

    db_table_dispenser = DbCsvTableDispenser()

    @abc.abstractmethod
    @pytest.fixture(scope="class")
    def connection_params(self) -> dict:
        raise NotImplementedError

    @pytest.fixture(scope="class")
    def edit_connection_params_case(self) -> EditConnectionParamsCase | None:
        return None

    @pytest.fixture(scope="function")
    def saved_connection_id(
        self,
        control_api_sync_client: SyncHttpClientBase,
        connection_params: dict,
        bi_headers: dict[str, str] | None = None,
    ) -> Generator[str, None, None]:
        with self.create_connection(
            control_api_sync_client=control_api_sync_client,
            connection_params=connection_params,
            bi_headers=bi_headers,
        ) as conn_id:
            yield conn_id

    @contextlib.contextmanager
    def create_connection(
        self,
        control_api_sync_client: SyncHttpClientBase,
        connection_params: dict,
        bi_headers: dict[str, str] | None = None,
    ) -> Generator[str, None, None]:
        data = dict(
            name=f"{self.conn_type.name} conn {uuid.uuid4()}",
            type=self.conn_type.name,
            **connection_params,
        )
        resp = control_api_sync_client.post(
            "/api/v1/connections",
            content_type="application/json",
            data=json.dumps(data),
            headers=bi_headers,
        )
        assert resp.status_code == 200, resp.json
        conn_id = resp.json["id"]

        yield conn_id

        control_api_sync_client.delete("/api/v1/connections/{}".format(conn_id))

    @pytest.fixture(scope="class")
    def sample_table(self, db: Db) -> DbTable:
        return self.db_table_dispenser.get_csv_table(db=db, spec=TABLE_SPEC_SAMPLE_SUPERSTORE)
