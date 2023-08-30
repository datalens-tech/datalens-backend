from __future__ import annotations

import os

import aiohttp.pytest_plugin
import aiohttp.test_utils
import pytest

from bi_file_secure_reader_lib.app import create_app

try:
    del aiohttp.pytest_plugin.loop
except AttributeError:
    pass

try:
    # Arcadia testing stuff
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None


ARCADIA_PREFIX = 'datalens/backend/lib/bi_file_secure_reader_lib/bi_file_secure_reader_lib_tests/db/'


@pytest.fixture
def web_app(loop, aiohttp_client):
    return loop.run_until_complete(aiohttp_client(create_app()))


@pytest.fixture(scope="session")
def excel_data():
    filename = 'data.xlsx'

    if yatest_common is not None:
        path = yatest_common.source_path(ARCADIA_PREFIX + filename)
    else:
        dirname = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(dirname, filename)

    with open(path, 'rb') as fd:
        return fd.read()
