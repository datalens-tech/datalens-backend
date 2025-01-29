from aiohttp.pytest_plugin import aiohttp_client
import pytest

from dl_testing.utils import get_root_certificates


@pytest.fixture(scope="session")
def root_certificates() -> bytes:
    return get_root_certificates()


# Imported fixtures
__all__ = [
    "aiohttp_client",
]
