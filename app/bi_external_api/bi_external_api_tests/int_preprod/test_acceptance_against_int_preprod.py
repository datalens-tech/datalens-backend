from __future__ import annotations

import os

import aiohttp
import pytest

from bi_api_commons_ya_team.models import YaTeamAuthData
from bi_defaults.environments import InternalTestingInstallation
from bi_external_api.enums import ExtAPIType
from bi_external_api.internal_api_clients.main import InternalAPIClients
from bi_external_api.testings import WorkbookOpsClient
from dl_api_commons.base_models import TenantCommon

from ..test_acceptance import ConnectionTestingData
from ..test_acceptance_ya_team import AcceptanceScenatioYaTeam


ENV_VAR_RUN_ACCEPTANCE_AGAINST_INT_PREPROD = "DL_EXT_API_TESTS_RUN_AGAINST_INT_PREPROD"


@pytest.mark.skipif(
    os.environ.get(ENV_VAR_RUN_ACCEPTANCE_AGAINST_INT_PREPROD) != "1",
    reason=f"Env var {ENV_VAR_RUN_ACCEPTANCE_AGAINST_INT_PREPROD!r} != 1",
)
class TestTrunkAcceptanceScenario(AcceptanceScenatioYaTeam):
    """
    Class to execute acceptance test against app deployed on internal preprod.
    Skipped by default. Should be un-skipped manually if required.
    Main case - check preprod before updating prod version.
    """

    _DO_ADD_EXC_MESSAGE = True

    @pytest.fixture
    def wb_id_value(self, pseudo_wb_path_value) -> str:
        return pseudo_wb_path_value

    @pytest.fixture(scope="session")
    def chyt_connection_testing_data(self, chyt_connection_testing_data_int_preprod) -> ConnectionTestingData:
        return chyt_connection_testing_data_int_preprod

    @pytest.fixture(scope="session")
    def ch_connection_testing_data(self, ch_connection_testing_data_int_preprod) -> ConnectionTestingData:
        return ch_connection_testing_data_int_preprod

    @pytest.fixture()
    def api(self, intranet_user_1_creds, loop):
        session = aiohttp.ClientSession()

        yield WorkbookOpsClient(
            # TODO FIX: BI-3005 Replace with dedicated balancer URL when will be ready
            base_url=InternalTestingInstallation.DATALENS_API_LB_UPLOADS_BASE_URL,
            auth_data=YaTeamAuthData(oauth_token=intranet_user_1_creds.token),
            tenant=TenantCommon(),
            api_type=ExtAPIType.YA_TEAM,
        )

        loop.run_until_complete(session.close())

    @pytest.fixture()
    def int_api_clients(self, bi_ext_api_int_preprod_int_api_clients) -> InternalAPIClients:
        return bi_ext_api_int_preprod_int_api_clients
