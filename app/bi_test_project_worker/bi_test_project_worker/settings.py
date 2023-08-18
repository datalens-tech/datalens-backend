from typing import Optional

import attr

from bi_configs.settings_loaders.meta_definition import s_attrib, required
from bi_configs.settings_submodels import RedisSettings
from bi_configs.utils import app_type_env_var_converter
from bi_configs.enums import AppType
from bi_configs.environments import CommonInstallation


@attr.s(frozen=True)
class TestProjectWorkerSettings:
    SENTRY_DSN: Optional[str] = s_attrib(  # type: ignore
        "SENTRY_DSN",
        fallback_factory=(
            lambda cfg: cfg.SENTRY_DSN_TEST_TASK_PROCESSOR_WORKER if isinstance(cfg, CommonInstallation) else None
        ),
        missing=None,
    )
    APP_TYPE: AppType = s_attrib(  # type: ignore
        "YENV_TYPE",
        is_app_cfg_type=True,
        env_var_converter=app_type_env_var_converter,
    )
    REDIS: RedisSettings = s_attrib(  # type: ignore
        "REDIS",
        fallback_factory=(
            lambda cfg: RedisSettings(  # type: ignore
                MODE=cfg.REDIS_PERSISTENT_MODE,
                CLUSTER_NAME=cfg.REDIS_PERSISTENT_CLUSTER_NAME,
                HOSTS=cfg.REDIS_PERSISTENT_HOSTS,
                PORT=cfg.REDIS_PERSISTENT_PORT,
                SSL=cfg.REDIS_PERSISTENT_SSL,
                DB=cfg.REDIS_TEST_PROJECT_WORKER_TASKS_DB,
                PASSWORD=required(str),
            ) if isinstance(cfg, CommonInstallation) else None
        ),
    )
