import pytest

import dl_app_api_base_tests.unit.conftest as unit_conftest
import dl_app_api_ext_testing


@pytest.fixture(name="ext_test_suite_settings")
def fixture_ext_test_suite_settings(
    app_settings: unit_conftest.AppSettings,
) -> dl_app_api_ext_testing.ExtTestSuiteSettings:
    return dl_app_api_ext_testing.ExtTestSuiteSettings(
        BASE_URL=f"http://{app_settings.HTTP_SERVER.HOST}:{app_settings.HTTP_SERVER.PORT}",
        APP_NAME=app_settings.APP_INFO.NAME,
        APP_VERSION=app_settings.APP_INFO.VERSION,
        EXPECTED_DYNCONFIG={"source_type": "null", "config": {}},
        READINESS_SUBSYSTEMS=[
            dl_app_api_ext_testing.ReadinessSubsystemSettings(
                NAME="readiness_resource.is_ready",
                CRITICAL=True,
            ),
            dl_app_api_ext_testing.ReadinessSubsystemSettings(
                NAME="non_critical_readiness_resource.is_ready",
                CRITICAL=False,
            ),
        ],
    )
