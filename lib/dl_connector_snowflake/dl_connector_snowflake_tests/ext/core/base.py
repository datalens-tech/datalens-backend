import asyncio
import datetime
from typing import (
    Any,
    Generator,
)

import pytest

from dl_core_testing.database import CoreDbConfig
from dl_core_testing.testcases.connection import BaseConnectionTestClass

from dl_connector_snowflake.core.constants import CONNECTION_TYPE_SNOWFLAKE
from dl_connector_snowflake.core.us_connection import ConnectionSQLSnowFlake
from dl_connector_snowflake.db_testing.engine_wrapper import SnowFlakeDbEngineConfig
import dl_connector_snowflake_tests.ext.config as test_config


class BaseSnowFlakeTestClass(BaseConnectionTestClass[ConnectionSQLSnowFlake]):
    conn_type = CONNECTION_TYPE_SNOWFLAKE
    core_test_config = test_config.CORE_TEST_CONFIG
    engine_config_cls = SnowFlakeDbEngineConfig

    @pytest.fixture(autouse=True)
    # FIXME: This fixture is a temporary solution for failing core tests when they are run together with api tests
    def loop(self, event_loop: asyncio.AbstractEventLoop) -> Generator[asyncio.AbstractEventLoop, None, None]:
        asyncio.set_event_loop(event_loop)
        yield event_loop
        # Attempt to cover an old version of pytest-asyncio:
        # https://github.com/pytest-dev/pytest-asyncio/commit/51d986cec83fdbc14fa08015424c79397afc7ad9
        asyncio.set_event_loop_policy(None)

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return test_config.DB_DSN

    @pytest.fixture(scope="class")
    def db(self, db_config: CoreDbConfig) -> Any:
        return None

    @pytest.fixture(scope="function")
    def connection_creation_params(self, sf_secrets) -> dict:
        return dict(
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
            **(dict(raw_sql_level=self.raw_sql_level) if self.raw_sql_level is not None else {}),
        )


class SnowFlakeTestClassWithExpiredRefreshToken(BaseSnowFlakeTestClass):
    @pytest.fixture(scope="function")
    def connection_creation_params_with_expired_refresh_token(self, sf_secrets) -> dict:
        # note: in fact any bad string could be used for this test as well ...
        return dict(
            account_name=sf_secrets.get_account_name(),
            user_name=sf_secrets.get_user_name(),
            client_id=sf_secrets.get_client_id(),
            client_secret=sf_secrets.get_client_secret(),
            db_name=sf_secrets.get_database(),
            schema=sf_secrets.get_schema(),
            warehouse=sf_secrets.get_warehouse(),
            refresh_token=sf_secrets.get_refresh_token_expired(),
            refresh_token_expire_time=None,
        )


class SnowFlakeTestClassWithRefreshTokenSoonToExpire(BaseSnowFlakeTestClass):
    @pytest.fixture(scope="function")
    def connection_creation_params_with_refresh_token_soon_to_expire(self, sf_secrets) -> dict:
        # note: in fact any bad string could be used for this test as well ...
        return dict(
            account_name=sf_secrets.get_account_name(),
            user_name=sf_secrets.get_user_name(),
            client_id=sf_secrets.get_client_id(),
            client_secret=sf_secrets.get_client_secret(),
            db_name=sf_secrets.get_database(),
            schema=sf_secrets.get_schema(),
            warehouse=sf_secrets.get_warehouse(),
            refresh_token=sf_secrets.get_refresh_token_x(),
            refresh_token_expire_time=datetime.datetime.now() + datetime.timedelta(days=2),
        )
