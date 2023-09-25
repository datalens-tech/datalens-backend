import asyncio
from typing import Callable

import pytest

from dl_connector_snowflake.auth import SFAuthProvider
from dl_connector_snowflake.core.adapters import construct_creator_func
from dl_connector_snowflake.core.dto import SnowFlakeConnDTO
from dl_connector_snowflake.core.target_dto import SnowFlakeConnTargetDTO
from dl_connector_snowflake.db_testing.engine_wrapper import SnowFlakeDbEngineConfig
from dl_connector_snowflake.formula.constants import SnowFlakeDialect as D
import dl_connector_snowflake_tests.ext.config as test_config  # noqa
from dl_formula_testing.testcases.base import FormulaConnectorTestBase


class SnowFlakeTestBase(FormulaConnectorTestBase):
    dialect = D.SNOWFLAKE
    eval_attempts = 2
    engine_config_cls = SnowFlakeDbEngineConfig

    supports_arrays = False
    supports_uuid = False
    null_casts_to_false = False

    # skip_custom_tz = True  # todo: probably could support with adjustments

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return test_config.DB_DSN

    @pytest.fixture(scope="class")
    def loop(self):
        return asyncio.get_event_loop()

    @pytest.fixture(scope="class")
    def _sf_target_dto(self, loop, sf_secrets):
        conn_dto = SnowFlakeConnDTO(
            conn_id=None,
            account_name=sf_secrets.get_account_name(),
            user_name=sf_secrets.get_user_name(),
            user_role=sf_secrets.get_user_role(),
            client_id=sf_secrets.get_client_id(),
            client_secret=sf_secrets.get_client_secret(),
            db_name=sf_secrets.get_database(),
            schema=sf_secrets.get_schema(),
            warehouse=sf_secrets.get_warehouse(),
            refresh_token=sf_secrets.get_refresh_token_x(),
            refresh_token_expire_time=None,
        )
        sf_auth_provider = SFAuthProvider.from_dto(conn_dto)
        access_token = loop.run_until_complete(sf_auth_provider.async_get_access_token())

        return SnowFlakeConnTargetDTO.from_dto(dto=conn_dto, access_token=access_token)

    @pytest.fixture(scope="class")
    def _sf_creator_func(self, _sf_target_dto):
        return construct_creator_func(_sf_target_dto)

    @pytest.fixture(scope="class")
    def engine_params(self, _sf_creator_func: Callable) -> dict:
        return dict(creator=_sf_creator_func)

    @pytest.fixture(scope="class")
    def engine_config(self, db_url: str, engine_params: dict, _sf_target_dto) -> SnowFlakeDbEngineConfig:
        return self.engine_config_cls(url=db_url, engine_params=engine_params, db_name=_sf_target_dto.db_name)
