import pytest
import statcommons.logs

import dl_logging


def pytest_configure(config: pytest.Config) -> None:
    statcommons.logs.LOGMUTATORS.apply(require=False)
    statcommons.logs.LOGMUTATORS.add_mutator("log_context", dl_logging.add_log_context)
