from __future__ import annotations

import os
from datetime import datetime

import grpc
import pytest
import pytz
import yaml

from bi_external_api.grpc_proxy.client import GrpcClientCtx
from bi_external_api.grpc_proxy.common import GHeaders
from bi_external_api.testing_no_deps import DomainScene
from ..test_scenario_grpc import GrpcAcceptanceScenarioDC

ENV_VAR_RUN_ACCEPTANCE_AGAINST_DC_PROD = "DL_EXT_API_TESTS_RUN_ACCEPTANCE_AGAINST_DC_PROD"


@pytest.mark.skipif(
    os.environ.get(ENV_VAR_RUN_ACCEPTANCE_AGAINST_DC_PROD) != "1",
    reason=f"Env var {ENV_VAR_RUN_ACCEPTANCE_AGAINST_DC_PROD!r} != 1"
)
class TestGRPCAcceptanceScenarioAgainstDeployedProdDoubleCloudExtAPI(GrpcAcceptanceScenarioDC):
    @pytest.fixture()
    def grpc_client_ctx(self, dc_rs_user_account) -> GrpcClientCtx:
        return GrpcClientCtx(
            endpoint="visualization.api.double.cloud",
            channel_credentials=grpc.ssl_channel_credentials(),
            headers=GHeaders({
                "authorization": f"Bearer {dc_rs_user_account.token}",
            }),
        )

    @pytest.fixture()
    def workbook_title(self) -> str:
        now = datetime.now(tz=pytz.timezone("Europe/Berlin"))
        return f"Autotests-{now.strftime('%y-%m-%d-%H:%M:%S.%f')}"

    @pytest.fixture()
    def project_id(self, dc_rs_project_id) -> str:
        return dc_rs_project_id

    @pytest.fixture()
    def domain_scene(self, secret_datalens_test_data) -> DomainScene:
        ch_data = yaml.safe_load(secret_datalens_test_data["INTEGRATION_TESTS_PREPROD_CH_1"])

        return DomainScene(
            ch_connection_data=dict(
                kind="clickhouse",
                host=ch_data["host"],
                port=ch_data["port"],
                username=ch_data["username"],
                secure=True,
                raw_sql_level="subselect",
                cache_ttl_sec=None,
            ),
            ch_connection_secret=dict(
                kind="plain",
                secret=ch_data["password"]
            ),
        )

    @pytest.mark.skip
    async def test_proxy_access_denied_error(self):
        pass
