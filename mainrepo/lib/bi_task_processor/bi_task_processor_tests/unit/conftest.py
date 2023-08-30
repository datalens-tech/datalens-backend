from statcommons.logs import LOGMUTATORS
from bi_api_commons.logging_config import add_ylog_context


def pytest_configure(config):  # noqa
    LOGMUTATORS.apply(require=False)
    LOGMUTATORS.add_mutator('ylog_context', add_ylog_context)
