from __future__ import annotations

import os

import aiohttp.pytest_plugin
import aiohttp.test_utils
import pytest

from dl_file_secure_reader_lib.app import create_app

try:
    del aiohttp.pytest_plugin.loop
except AttributeError:
    pass


@pytest.fixture
def web_app(loop, aiohttp_client):
    return loop.run_until_complete(aiohttp_client(create_app()))


@pytest.fixture(scope="session")
def excel_data():
    filename = "data.xlsx"

    dirname = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(dirname, filename)

    with open(path, "rb") as fd:
        return fd.read()
