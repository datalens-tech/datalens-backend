import asyncio
from collections.abc import Generator

import pytest

from dl_core_testing.configuration import CoreTestEnvironmentConfiguration
import dl_core_tests.db.config as test_config
from dl_testing.utils import get_root_certificates


@pytest.fixture(scope="session")
def core_test_config() -> CoreTestEnvironmentConfiguration:
    return test_config.CORE_TEST_CONFIG


@pytest.fixture(scope="session")
def root_certificates() -> bytes:
    return get_root_certificates()


@pytest.fixture(autouse=True)
def loop(event_loop: asyncio.AbstractEventLoop) -> Generator[asyncio.AbstractEventLoop, None, None]:
    asyncio.set_event_loop(event_loop)
    yield event_loop
    asyncio.set_event_loop_policy(None)
