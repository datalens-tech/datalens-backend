import contextlib
from typing import (
    Generator,
    Optional,
)

import attr

from dl_configs.rqe import (
    RQEBaseURL,
    RQEConfig,
)
from dl_core_testing.fixture_server_runner import WSGIRunner


@attr.s
class RQEConfigurationMaker:
    ext_query_executer_secret_key: str = attr.ib(kw_only=True)
    core_connector_whitelist: Optional[list[str]] = attr.ib(kw_only=True, default=None)

    @contextlib.contextmanager
    def sync_rqe_netloc_subprocess_cm(self) -> Generator[RQEBaseURL, None, None]:
        env = dict(
            EXT_QUERY_EXECUTER_SECRET_KEY=self.ext_query_executer_secret_key,
            DEV_LOGGING="1",
        )
        if self.core_connector_whitelist is not None:
            env["CORE_CONNECTOR_WHITELIST"] = ",".join(self.core_connector_whitelist)

        with WSGIRunner(
            module="dl_core.bin.query_executor_sync",
            callable="app",
            ping_path="/ping",
            env=env,
        ) as runner:
            yield RQEBaseURL(  # type: ignore  # TODO: fix compatibility of models using `s_attrib` with mypy
                host=runner.bind_addr,
                port=runner.bind_port,
            )

    @contextlib.contextmanager
    def async_rqe_netloc_subprocess_cm(self) -> Generator[RQEBaseURL, None, None]:
        # TODO FIX: Implement async RQE in subprocess
        yield RQEBaseURL(  # type: ignore  # TODO: fix compatibility of models using `s_attrib` with mypy
            host="127.0.0.1",
            port=65500,
        )

    @contextlib.contextmanager
    def rqe_config_subprocess_cm(self) -> Generator[RQEConfig, None, None]:
        with (
            self.sync_rqe_netloc_subprocess_cm() as sync_rqe_netloc,
            self.async_rqe_netloc_subprocess_cm() as async_rqe_netloc,
        ):
            yield RQEConfig(  # type: ignore  # TODO: fix compatibility of models using `s_attrib` with mypy
                hmac_key=self.ext_query_executer_secret_key.encode(),
                ext_sync_rqe=sync_rqe_netloc,
                ext_async_rqe=async_rqe_netloc,
                int_sync_rqe=sync_rqe_netloc,
                int_async_rqe=async_rqe_netloc,
            )
