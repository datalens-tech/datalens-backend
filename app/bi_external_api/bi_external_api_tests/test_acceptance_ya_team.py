from __future__ import annotations

import attr
import pytest

from bi_external_api.domain import external as ext
from bi_external_api.ext_examples import SuperStoreExtDSBuilder

from .test_acceptance import (
    AcceptanceScenario,
    ConnectionTestingData,
    CreatedConnectionTestingData,
)


class AcceptanceScenatioYaTeam(AcceptanceScenario):
    @pytest.fixture()
    def wb_id_value(self) -> str:
        raise NotImplementedError()

    # Final fixtures. MUST NOT be overridden
    @pytest.fixture()
    async def wb_id(self, api, wb_id_value) -> str:
        await api.create_fake_workbook(ext.FakeWorkbookCreateRequest(workbook_id=wb_id_value))
        return wb_id_value

    @pytest.fixture(scope="session")
    def chyt_connection_testing_data(self) -> ConnectionTestingData:
        pytest.skip("No CHYT connection data provided for scenario")

    @pytest.fixture(scope="session")
    def chyt_connection_testing_data_ensured(self, chyt_connection_testing_data) -> ConnectionTestingData:
        return chyt_connection_testing_data

    @pytest.fixture()
    async def chyt_ua_connection(
        self, api, wb_id: str, chyt_connection_testing_data_ensured
    ) -> CreatedConnectionTestingData:
        orig_conn = chyt_connection_testing_data_ensured.connection
        assert isinstance(orig_conn, ext.CHYTConnection)

        effective_data = attr.evolve(
            chyt_connection_testing_data_ensured,
            connection=ext.CHYTUserAuthConnection(
                cache_ttl_sec=orig_conn.cache_ttl_sec,
                clique_alias=orig_conn.clique_alias,
                cluster=orig_conn.cluster,
                raw_sql_level=orig_conn.raw_sql_level,
            ),
            secret=None,
        )
        return await self.create_connection(api, wb_id, ext.CHYTUserAuthConnection, "chyt_ua_conn", effective_data)

    @pytest.fixture()
    async def chyt_connection(
        self, api, wb_id: str, chyt_connection_testing_data_ensured
    ) -> CreatedConnectionTestingData:
        return await self.create_connection(
            api, wb_id, ext.CHYTConnection, "chyt_conn", chyt_connection_testing_data_ensured
        )

    @pytest.fixture(
        params=[
            "chyt_connection",
            # TODO FIX: Uncomment when scope for CHYT will be added for oAuth token of testing robot
            # "chyt_ua_connection",
            "ch_connection",
        ]
    )
    def conn_td(self, request) -> ext.EntryInfo:
        """
        Dispatching fixture that return any connection
        """
        return request.getfixturevalue(request.param)

    #
    # Extra tests
    #
    @pytest.mark.asyncio
    async def test_create_wb_with_connections(self, api, chyt_connection_testing_data_ensured, wb_id_value):
        conn_data = chyt_connection_testing_data_ensured.connection
        conn_secret = chyt_connection_testing_data_ensured.secret

        conn_name = "chyt_on_create"

        wb_create_info = await api.create_fake_workbook(
            ext.FakeWorkbookCreateRequest(
                workbook_id=wb_id_value,
                workbook=ext.WorkbookConnectionsOnly(
                    connections=[ext.ConnectionInstance(name=conn_name, connection=conn_data)]
                ),
                connection_secrets=[ext.ConnectionSecret(conn_name=conn_name, secret=conn_secret)],
            )
        )
        conn_info = wb_create_info.created_entries_info[0]

        # Check that connection was successfully created and dataset can be validated against it
        ds = SuperStoreExtDSBuilder(conn_name=conn_info.name).do_add_default_fields().build_instance("main_ds")
        await api.write_workbook(
            ext.WorkbookWriteRequest(
                workbook_id=wb_id_value,
                workbook=ext.WorkBook(datasets=[ds], charts=[], dashboards=[]),
            )
        )
