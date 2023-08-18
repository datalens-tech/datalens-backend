import contextlib
from typing import Generator

import attr

from bi_configs.enums import RedisMode
from bi_configs.rqe import RQEBaseURL, RQEConfig
from bi_configs.settings_submodels import RedisSettings

from bi_core_testing.fixture_server_runner import WSGIRunner

from bi_api_lib.app_common import LegacySRFactoryBuilder
from bi_api_lib.app.control_api.app import ControlApiAppFactory
from bi_api_lib.app.data_api.app import DataApiAppFactory
from bi_api_lib_testing.configuration import BiApiTestEnvironmentConfiguration


@attr.s
class RQEConfigurationMaker:
    bi_test_config: BiApiTestEnvironmentConfiguration = attr.ib(kw_only=True)

    @contextlib.contextmanager
    def sync_rqe_netloc_subprocess_cm(self) -> Generator[RQEBaseURL, None, None]:
        with WSGIRunner(
                module='bi_core.bin.query_executor_sync',
                callable='app',
                ping_path='/ping',
                env=dict(EXT_QUERY_EXECUTER_SECRET_KEY=self.bi_test_config.ext_query_executer_secret_key),
        ) as runner:
            yield RQEBaseURL(  # type: ignore  # TODO: fix compatibility of models using `s_attrib` with mypy
                host=runner.bind_addr,
                port=runner.bind_port,
            )

    @contextlib.contextmanager
    def async_rqe_netloc_subprocess_cm(self) -> Generator[RQEBaseURL, None, None]:
        yield RQEBaseURL(  # type: ignore  # TODO: fix compatibility of models using `s_attrib` with mypy
            host="127.0.0.1",
            port=65500,
        )

    @contextlib.contextmanager
    def rqe_config_subprocess_cm(self) -> Generator[RQEConfig, None, None]:
        with (
                self.sync_rqe_netloc_subprocess_cm() as sync_rqe_netloc,
                self.async_rqe_netloc_subprocess_cm() as async_rqe_netloc
        ):
            yield RQEConfig(  # type: ignore  # TODO: fix compatibility of models using `s_attrib` with mypy
                hmac_key=self.bi_test_config.ext_query_executer_secret_key.encode(),
                ext_sync_rqe=sync_rqe_netloc,
                ext_async_rqe=async_rqe_netloc,
                int_sync_rqe=sync_rqe_netloc,
                int_async_rqe=async_rqe_netloc,
            )


@attr.s
class RedisSettingMaker:
    bi_test_config: BiApiTestEnvironmentConfiguration = attr.ib(kw_only=True)

    def get_redis_settings(self, db: int) -> RedisSettings:
        return RedisSettings(  # type: ignore  # TODO: fix compatibility of models using `s_attrib` with mypy
            MODE=RedisMode.single_host,
            CLUSTER_NAME='',
            HOSTS=(self.bi_test_config.redis_host,),
            PORT=self.bi_test_config.redis_port,
            DB=db,
            PASSWORD=self.bi_test_config.redis_password,
        )

    def get_redis_settings_default(self) -> RedisSettings:
        return self.get_redis_settings(self.bi_test_config.redis_db_default)

    def get_redis_settings_cache(self) -> RedisSettings:
        return self.get_redis_settings(self.bi_test_config.redis_db_cache)

    def get_redis_settings_mutation(self) -> RedisSettings:
        return self.get_redis_settings(self.bi_test_config.redis_db_mutation)

    def get_redis_settings_arq(self) -> RedisSettings:
        return self.get_redis_settings(self.bi_test_config.redis_db_arq)


class TestingControlApiAppFactory(ControlApiAppFactory, LegacySRFactoryBuilder):
    """Management API app factory for tests"""


class TestingDataApiAppFactory(DataApiAppFactory, LegacySRFactoryBuilder):
    """Data API app factory for tests"""
