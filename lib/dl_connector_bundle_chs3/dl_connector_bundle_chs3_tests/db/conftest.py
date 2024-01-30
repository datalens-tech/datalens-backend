from typing import (
    Any,
    Optional,
)

import pytest

from dl_api_lib_testing.initialization import initialize_api_lib_test

from dl_connector_bundle_chs3.chs3_base.core.us_connection import BaseFileS3Connection
from dl_connector_bundle_chs3_tests.db.config import API_TEST_CONFIG


def pytest_configure(config):  # noqa
    initialize_api_lib_test(pytest_config=config, api_test_config=API_TEST_CONFIG)


@pytest.fixture(autouse=True)
def patch_get_full_s3_filename(monkeypatch: pytest.MonkeyPatch) -> None:
    def _patched(self: Any, s3_filename_suffix: str) -> str:
        return s3_filename_suffix

    monkeypatch.setattr(BaseFileS3Connection, "get_full_s3_filename", _patched)
