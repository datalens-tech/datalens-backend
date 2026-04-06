import pytest

import dl_app_api_base_tests.settings as test_settings


@pytest.fixture(name="settings")
def fixture_settings() -> test_settings.Settings:
    return test_settings.Settings()
