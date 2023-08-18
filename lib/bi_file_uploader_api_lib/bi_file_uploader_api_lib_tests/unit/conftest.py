import pytest


@pytest.fixture(scope='function')
def fill_secret_env_variables(monkeypatch):
    env_names = (
        'REDIS_ARQ_PASSWORD',
        'REDIS_APP_PASSWORD',
        'S3_ACCESS_KEY_ID',
        'S3_SECRET_ACCESS_KEY',
        'CSRF_SECRET',
        'FILE_UPLOADER_MASTER_TOKEN',
    )
    for var_name in env_names:
        monkeypatch.setenv(var_name, 'dummy_secret')
