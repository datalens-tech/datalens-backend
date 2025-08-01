import pytest
from statcommons.logs import LOGMUTATORS

from dl_api_commons.logging_config import add_log_context


def pytest_configure(config: pytest.Config) -> None:
    LOGMUTATORS.apply(require=False)
    LOGMUTATORS.add_mutator("log_context", add_log_context)
