import pytest

from dl_file_uploader_lib_tests.ext.settings import Settings


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings()
