from __future__ import annotations

from typing import ClassVar

import pytest

from bi_external_api.enums import ExtAPIType
from bi_external_api.internal_api_clients.main import InternalAPIClients
from bi_external_api.workbook_ops.facade import WorkbookOpsFacade
from ..test_acceptance import ConnectionTestingData
from ..test_acceptance_ya_team import AcceptanceScenatioYaTeam


class TestTrunkAcceptanceScenario(AcceptanceScenatioYaTeam):
    _DO_ADD_EXC_MESSAGE: ClassVar[bool] = True

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
    def api(self, bi_ext_api_int_preprod_int_api_clients, wb_ctx_loader) -> WorkbookOpsFacade:
        return WorkbookOpsFacade(
            internal_api_clients=bi_ext_api_int_preprod_int_api_clients,
            workbook_ctx_loader=wb_ctx_loader,
            api_type=ExtAPIType.YA_TEAM,
            do_add_exc_message=self._DO_ADD_EXC_MESSAGE,
        )


    @pytest.fixture()
    def int_api_clients(self, bi_ext_api_int_preprod_int_api_clients) -> InternalAPIClients:
        return bi_ext_api_int_preprod_int_api_clients
