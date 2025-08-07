import asyncio
import os

from aiohttp.pytest_plugin import (
    AiohttpClient,
    TestClient,
    aiohttp_client,
)
import pytest

from dl_file_secure_reader_lib.app import create_app


@pytest.fixture
def web_app(loop: asyncio.AbstractEventLoop, aiohttp_client: AiohttpClient) -> TestClient:
    return loop.run_until_complete(aiohttp_client(create_app()))


@pytest.fixture(scope="session")
def excel_data() -> bytes:
    filename = "data.xlsx"

    dirname = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(dirname, filename)

    with open(path, "rb") as fd:
        return fd.read()


# Imported fixtures
__all__ = [
    "aiohttp_client",
]
