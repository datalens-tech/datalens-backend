import pytest

from dl_testing.utils import get_root_certificates


pytest_plugins = ("aiohttp.pytest_plugin",)  # and it, in turn, includes 'pytest_asyncio.plugin'


@pytest.fixture(scope="function")
def root_certificates() -> bytes:
    return get_root_certificates()
