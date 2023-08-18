from bi_configs.settings_loaders.loader_env import load_settings_from_env_with_fallback

from bi_file_uploader_api_lib.settings import FileUploaderAPISettings


def test_load_settings(fill_env_variables, fill_secret_env_variables):
    settings = load_settings_from_env_with_fallback(FileUploaderAPISettings)
    assert settings

    assert settings.SENTRY_DSN is None
