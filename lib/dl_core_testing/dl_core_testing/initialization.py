from pytest import Config
from statcommons.logs import LOGMUTATORS

from dl_core.loader import load_core_lib
from dl_core.logging_config import add_log_context_scoped
from dl_core_testing.configuration import CoreTestEnvironmentConfiguration
from dl_core_testing.environment import prepare_united_storage_from_config
from dl_db_testing.loader import load_db_testing_lib


def initialize_core_test(pytest_config: Config, core_test_config: CoreTestEnvironmentConfiguration) -> None:
    # Configure pytest itself
    pytest_config.addinivalue_line("markers", "slow: ...")
    pytest_config.addinivalue_line("markers", "yt: ...")

    # Add Log context to logging records (not only in format phase)
    LOGMUTATORS.apply(require=False)
    # TODO FIX: Replace with add_log_context after all tests will be refactored to use unscoped log context
    LOGMUTATORS.add_mutator("log_context_scoped", add_log_context_scoped)

    # Prepare US
    prepare_united_storage_from_config(core_test_config.get_us_config())

    # Initialize this and other libraries
    load_db_testing_lib()
    load_core_lib(core_lib_config=core_test_config.get_core_library_config())
