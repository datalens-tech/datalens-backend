from bi_configs.settings_loaders.loader_env import load_settings_from_env_with_fallback
from bi_external_api.enums import ExtAPIType
from bi_external_api.settings import ExternalAPISettings

from bi_testing.utils import override_env_cm


def test_min_env():
    env_vars = {
        "YENV_TYPE": "tests",
        "YENV_NAME": "intranet",
        "DATASET_CONTROL_PLANE_API_BASE_URL": "http://example.com",
        "US_BASE_URL": "http://example.com",
    }

    with override_env_cm(to_set=env_vars, purge=True):
        settings: ExternalAPISettings = load_settings_from_env_with_fallback(ExternalAPISettings)

    assert settings.API_TYPE == ExtAPIType.CORE
    assert settings.DO_ADD_EXC_MESSAGE is True


def test_dc_defaults_env():
    env_vars = {
        "YENV_TYPE": "datacloud",
        "YENV_NAME": "datacloud",
        "EXT_API_TYPE": "dc",
        "DATASET_CONTROL_PLANE_API_BASE_URL": "http://example.com",
        "US_BASE_URL": "http://example.com",
        "DO_ADD_EXC_MESSAGE": "1",
    }

    with override_env_cm(to_set=env_vars, purge=True):
        settings: ExternalAPISettings = load_settings_from_env_with_fallback(ExternalAPISettings)

    assert settings.API_TYPE == ExtAPIType.DC
    assert settings.DO_ADD_EXC_MESSAGE is True
