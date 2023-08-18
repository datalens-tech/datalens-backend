from __future__ import annotations

import os
import pytest
import pytz
from datetime import datetime
from typing import ClassVar

from bi_external_api.domain import external as ext
from bi_external_api.enums import ExtAPIType
from bi_external_api.internal_api_clients.main import InternalAPIClients
from bi_external_api.workbook_ops.facade import WorkbookOpsFacade
from bi_external_api_tests.test_acceptance import ConnectionTestingData
from bi_external_api_tests.test_acceptance_ya_team import AcceptanceScenatioYaTeam
from . import conftest

_DO_LAUNCH = os.environ.get(conftest.ENV_KEY_DO_NOT_SKIP)


@pytest.mark.skipif(
    _DO_LAUNCH is None or _DO_LAUNCH == "0",
    reason=f"{conftest.ENV_KEY_DO_NOT_SKIP} env var is not set or is 0",
)
class TestTrunkAcceptanceScenario(AcceptanceScenatioYaTeam):
    _DO_ADD_EXC_MESSAGE: ClassVar[bool] = True

    @pytest.fixture
    def wb_id_value(self) -> str:
        now = datetime.now(tz=pytz.timezone("Europe/Moscow"))
        return f"ext_api_tests/{now.strftime('%y-%m-%d-%H:%M:%S.%f')}"

    @pytest.fixture(scope="session")
    def chyt_connection_testing_data(
            self,
            env_param_getter,
    ) -> ConnectionTestingData:
        return ConnectionTestingData(
            connection=ext.CHYTConnection(
                raw_sql_level=ext.RawSQLLevel.subselect,
                cache_ttl_sec=None,
                cluster="hahn",
                clique_alias="*ch_datalens",
            ),
            secret=ext.PlainSecret(env_param_getter.get_str_value("YT_OAUTH")),
            target_db_name=None,
            sample_super_store_schema_name=None,
            sample_super_store_table_name="//home/yandexbi/samples/sample_superstore",
            perform_tables_validation=False,
            perform_tables_fix=False,
        )

    @pytest.fixture()
    def api(self, bi_ext_api_int_preprod_int_api_clients, wb_ctx_loader) -> WorkbookOpsFacade:
        return WorkbookOpsFacade(
            internal_api_clients=bi_ext_api_int_preprod_int_api_clients,
            workbook_ctx_loader=wb_ctx_loader,
            api_type=ExtAPIType.CORE,
            do_add_exc_message=self._DO_ADD_EXC_MESSAGE,
        )

    @pytest.fixture()
    def int_api_clients(self, bi_ext_api_int_preprod_int_api_clients) -> InternalAPIClients:
        return bi_ext_api_int_preprod_int_api_clients
