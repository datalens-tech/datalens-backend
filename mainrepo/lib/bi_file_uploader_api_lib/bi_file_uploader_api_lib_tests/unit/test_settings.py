from bi_configs.crypto_keys import get_dummy_crypto_keys_config
from bi_configs.settings_loaders.loader_env import load_settings_from_env_with_fallback

from bi_file_uploader_api_lib.settings import FileUploaderAPISettings


def test_load_settings(monkeypatch, fill_secret_env_variables):
    monkeypatch.setenv('YENV_TYPE', 'tests')  # TODO remove
    monkeypatch.setenv('DL_CRY_ACTUAL_KEY_ID', 'tests')
    monkeypatch.setenv('DL_CRY_KEY_VAL_ID_tests', get_dummy_crypto_keys_config().actual_key_id)

    settings = load_settings_from_env_with_fallback(FileUploaderAPISettings)
    assert settings

    assert settings.SENTRY_DSN is None
