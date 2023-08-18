import datetime
from typing import Any

import pytest

from bi_core_testing.testcases.connection import BaseConnectionTestClass
from bi_core_testing.database import CoreDbConfig
from bi_core.us_manager.us_manager_sync import SyncUSManager

from bi_connector_snowflake.core.constants import CONNECTION_TYPE_SNOWFLAKE
from bi_connector_snowflake.core.testing.connection import make_snowflake_saved_connection
from bi_connector_snowflake.core.us_connection import ConnectionSQLSnowFlake
from bi_connector_snowflake.db_testing.engine_wrapper import SnowFlakeDbEngineConfig

import bi_connector_snowflake_tests.ext.config as test_config  # noqa


class BaseSnowFlakeTestClass(BaseConnectionTestClass[ConnectionSQLSnowFlake]):
    conn_type = CONNECTION_TYPE_SNOWFLAKE
    core_test_config = test_config.CORE_TEST_CONFIG
    engine_config_cls = SnowFlakeDbEngineConfig

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return test_config.DB_DSN

    @pytest.fixture(scope='class')
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

    @pytest.fixture(scope="function")
    def saved_connection(
        self,
        sync_us_manager: SyncUSManager,
        connection_creation_params: dict,
    ) -> ConnectionSQLSnowFlake:
        conn = make_snowflake_saved_connection(sync_usm=sync_us_manager, **connection_creation_params)
        return conn

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

    @pytest.fixture(scope="function")
    def saved_connection_with_expired_refresh_token(
        self,
        sync_us_manager: SyncUSManager,
        connection_creation_params_with_expired_refresh_token: dict,
    ) -> ConnectionSQLSnowFlake:
        conn = make_snowflake_saved_connection(sync_usm=sync_us_manager, **connection_creation_params_with_expired_refresh_token)
        return conn

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

    @pytest.fixture(scope="function")
    def saved_connection_with_refresh_token_soon_to_expire(
        self,
        sync_us_manager: SyncUSManager,
        connection_creation_params_with_refresh_token_soon_to_expire: dict,
    ) -> ConnectionSQLSnowFlake:
        conn = make_snowflake_saved_connection(sync_usm=sync_us_manager,
                                               **connection_creation_params_with_refresh_token_soon_to_expire)
        return conn
