from __future__ import annotations

from typing import ClassVar

import grpc
import pytest

from bi_external_api.converter.workbook_ctx_loader import WorkbookContextLoader
from bi_external_api.domain import external as ext
from bi_external_api.enums import ExtAPIType
from bi_external_api.grpc_proxy.client import GrpcClientCtx
from bi_external_api.grpc_proxy.common import GHeaders
from bi_external_api.internal_api_clients.main import InternalAPIClients
from bi_external_api.testing_no_deps import DomainScene
from bi_external_api.workbook_ops.facade import WorkbookOpsFacade
from bi_testing.sa_factories import DisposableYCServiceAccountFactory
from ..test_acceptance_dc import AcceptanceScenarioDC
from ..test_scenario_grpc import GrpcAcceptanceScenarioDC


class TestTrunkAcceptanceScenario(AcceptanceScenarioDC):
    _DO_ADD_EXC_MESSAGE: ClassVar[bool] = False

    @pytest.fixture()
    def wb_title(self, workbook_name) -> str:
        return workbook_name

    @pytest.fixture()
    def api(self, int_api_clients) -> WorkbookOpsFacade:
        return WorkbookOpsFacade(
            internal_api_clients=int_api_clients,
            workbook_ctx_loader=WorkbookContextLoader(
                internal_api_clients=int_api_clients,
                use_workbooks_api=True,
            ),
            do_add_exc_message=self._DO_ADD_EXC_MESSAGE,
            api_type=ExtAPIType.CORE,
        )

    @pytest.fixture()
    def int_api_clients(self, bi_ext_api_dc_preprod_int_api_clients) -> InternalAPIClients:
        return bi_ext_api_dc_preprod_int_api_clients

    @pytest.fixture(scope="session")
    def ch_connection_testing_data(self, bi_ext_api_dc_preprod_ch_connection_testing_data):
        return bi_ext_api_dc_preprod_ch_connection_testing_data


@pytest.mark.asyncio
async def test_auth_ok_get_workbook(workbook_id, bi_ext_api_dc_preprod_http_client):
    rs = await bi_ext_api_dc_preprod_http_client.dc_read_workbook(ext.DCOpWorkbookGetRequest(workbook_id=workbook_id))
    assert rs.workbook == ext.WorkBook.create_empty()


class TestWorkbookBasicData:
    @pytest.fixture(scope="session")
    def workbook_meaningful_title(self) -> str:
        return "Autotests workbook title"

    @pytest.fixture(scope="function")
    async def meaningful_workbook_id(
            self,
            bi_ext_api_dc_preprod_int_api_clients,
            workbook_meaningful_title
    ) -> str:
        cli = bi_ext_api_dc_preprod_int_api_clients.us

        wb_id = await cli.create_workbook(workbook_meaningful_title)
        yield wb_id
        await cli.delete_workbook(wb_id)

    @pytest.mark.asyncio
    async def test_get_meaningful_workbook(
            self,
            meaningful_workbook_id,
            workbook_meaningful_title,
            bi_ext_api_dc_preprod_http_client,
            dc_rs_project_id
    ):
        rs = await bi_ext_api_dc_preprod_http_client.dc_read_workbook(ext.DCOpWorkbookGetRequest(
            workbook_id=meaningful_workbook_id
        ))
        assert rs == ext.DCOpWorkbookGetResponse(
            workbook=ext.WorkBook.create_empty(),
            id=meaningful_workbook_id,
            title=workbook_meaningful_title,
            project_id=dc_rs_project_id
        )


@pytest.mark.asyncio
async def test_auth_bad_get_workbook(workbook_id, bi_ext_api_dc_preprod_http_client_no_creds):
    cli = bi_ext_api_dc_preprod_http_client_no_creds
    with pytest.raises(
            AssertionError,
            match=f"^GOT UNEXPECTED STATUS CODE 401\n"
    ):
        await cli.read_workbook(ext.WorkbookReadRequest(workbook_id=workbook_id))


class TestGRPCAcceptanceScenarioAgainstLocalExtAPI(GrpcAcceptanceScenarioDC):
    ALLOW_EXC_MSG_AND_STACK_TRACE_IN_ERRORS = True
    CHECK_GRPC_LOGS = True

    @pytest.fixture()
    def grpc_client_ctx(self, bi_ext_api_grpc_against_dc_preprod, dc_rc_user_account) -> GrpcClientCtx:
        return GrpcClientCtx(
            endpoint=bi_ext_api_grpc_against_dc_preprod.grpc_endpoint,
            channel_credentials=None,
            headers=GHeaders({
                "authorization": f"Bearer {dc_rc_user_account.token}",
            }),
        )

    @pytest.fixture()
    def grpc_client_ctx_disposable_sa(
            self,
            ext_sys_helpers_per_session,
            bi_ext_api_grpc_against_dc_preprod,
            integration_tests_admin_sa,
            project_id
    ) -> GrpcClientCtx:
        iam_rm_client = ext_sys_helpers_per_session.get_iam_rm_client(integration_tests_admin_sa.token)

        sa_factory = DisposableYCServiceAccountFactory(
            iam_client=iam_rm_client,
            folder_id=project_id,
            ext_sys_helpers=ext_sys_helpers_per_session,
        )
        account = sa_factory.create(tuple())
        return GrpcClientCtx(
            endpoint=bi_ext_api_grpc_against_dc_preprod.grpc_endpoint,
            channel_credentials=None,
            headers=GHeaders({
                "authorization": f"Bearer {account.token}",
            }),
        )

    @pytest.fixture()
    def workbook_title(self, workbook_name) -> str:
        return workbook_name

    @pytest.fixture()
    def project_id(self, dc_rs_project_id) -> str:
        return dc_rs_project_id

    @pytest.fixture()
    def domain_scene(self, bi_ext_api_dc_preprod_ch_connection_testing_data) -> DomainScene:
        conn_data = bi_ext_api_dc_preprod_ch_connection_testing_data
        return DomainScene(
            ch_connection_data=dict(
                kind="clickhouse",
                host=conn_data.connection.host,
                port=conn_data.connection.port,
                username=conn_data.connection.username,
                secure=True,
                raw_sql_level="subselect",
                cache_ttl_sec=None,
            ),
            ch_connection_secret=dict(
                kind="plain",
                secret=conn_data.secret.secret
            ),
        )


class TestGRPCAcceptanceScenarioAgainstDeployedPreprodDoubleCloudExtAPI(GrpcAcceptanceScenarioDC):
    ALLOW_EXC_MSG_AND_STACK_TRACE_IN_ERRORS = True

    @pytest.fixture()
    def grpc_client_ctx(self, dc_rc_user_account) -> GrpcClientCtx:
        return GrpcClientCtx(
            endpoint="visualization.api.yadc.io",
            channel_credentials=grpc.ssl_channel_credentials(),
            headers=GHeaders({
                "authorization": f"Bearer {dc_rc_user_account.token}",
            }),
        )

    @pytest.fixture()
    def grpc_client_ctx_disposable_sa(
            self,
            integration_tests_folder_id,
            ext_sys_helpers_per_session,
            integration_tests_admin_sa,
    ) -> GrpcClientCtx:
        iam_rm_client = ext_sys_helpers_per_session.get_iam_rm_client(integration_tests_admin_sa.token)
        sa_factory = DisposableYCServiceAccountFactory(
            iam_client=iam_rm_client,
            folder_id=integration_tests_folder_id,
            ext_sys_helpers=ext_sys_helpers_per_session,
        )
        account = sa_factory.create(tuple())
        return GrpcClientCtx(
            endpoint="visualization.api.yadc.io",
            channel_credentials=grpc.ssl_channel_credentials(),
            headers=GHeaders({
                "authorization": f"Bearer {account.token}",
            }),
        )

    @pytest.fixture()
    def workbook_title(self, workbook_name) -> str:
        return workbook_name

    @pytest.fixture()
    def project_id(self, dc_rs_project_id) -> str:
        return dc_rs_project_id

    @pytest.fixture()
    def domain_scene(self, bi_ext_api_dc_preprod_ch_connection_testing_data) -> DomainScene:
        conn_data = bi_ext_api_dc_preprod_ch_connection_testing_data
        return DomainScene(
            ch_connection_data=dict(
                kind="clickhouse",
                host=conn_data.connection.host,
                port=conn_data.connection.port,
                username=conn_data.connection.username,
                secure=True,
                raw_sql_level="subselect",
                cache_ttl_sec=None,
            ),
            ch_connection_secret=dict(
                kind="plain",
                secret=conn_data.secret.secret
            ),
        )
