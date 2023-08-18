import asyncio

import aiohttp.pytest_plugin
import aiohttp.test_utils
import pytest

pytest_plugins = (  # installs required fixtures for secrets  # TODO: refactor
    'aiohttp.pytest_plugin',
)

try:
    del aiohttp.pytest_plugin.loop
except AttributeError:
    pass


@pytest.fixture
def loop(event_loop):
    asyncio.set_event_loop(event_loop)
    yield event_loop
    # Attempt to cover an old version of pytest-asyncio:
    # https://github.com/pytest-dev/pytest-asyncio/commit/51d986cec83fdbc14fa08015424c79397afc7ad9
    asyncio.set_event_loop_policy(None)
