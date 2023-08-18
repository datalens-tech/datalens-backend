import os
from typing import Optional, Mapping

import attr

from bi_testing_db_provision.common_config_models import PGConfig


@attr.s
class AppSettings:
    pg_config: PGConfig = attr.ib()
    launch_api: bool = attr.ib(default=True)
    launch_workers: bool = attr.ib(default=True)
    workers_id_prefix: Optional[str] = attr.ib(default=None)
    workers_count_provisioning: int = attr.ib(default=1)
    workers_count_recycling: int = attr.ib(default=1)


DEVELOPMENT_SETTINGS = AppSettings(
    # Keep in sync with docker-compose.yml
    pg_config=PGConfig(
        host_list=["127.0.0.1"],
        port=50913,
        database="common_test",
        user='dbp_db_user',
        password='qwerty',
        ssl_mode='disable',
        connect_timeout=2,
    ),
    workers_id_prefix='dev_worker',
)


def get_int_testing_settings(env: Optional[Mapping[str, str]] = None) -> AppSettings:
    actual_env: Mapping[str, str]

    if env is not None:
        actual_env = env
    else:
        actual_env = os.environ

    return AppSettings(
        pg_config=PGConfig(
            host_list=['sas-wnwyn9eswgmj1q2q.db.yandex.net', ],
            port=6432,
            database='main',
            user='main_user',
            password=actual_env['PG_PASSWORD'],
            ssl_mode='verify-full',
            connect_timeout=2,
        ),
        # TODO FIX: Take from env
        launch_workers=True,
        # TODO FIX: Take from env
        launch_api=True,
        workers_id_prefix=None,  # use hostname
    )


def get_app_settings_from_env(env_name: Optional[str] = None) -> AppSettings:
    # TODO FIX: Take default from environment name from environment variable
    effective_env_name: Optional[str] = os.environ.get('YENV_TYPE')

    if effective_env_name == 'development':
        return DEVELOPMENT_SETTINGS
    elif effective_env_name == 'int-testing':
        return get_int_testing_settings()

    raise AssertionError(f"Got no settings for env {env_name!r}")
