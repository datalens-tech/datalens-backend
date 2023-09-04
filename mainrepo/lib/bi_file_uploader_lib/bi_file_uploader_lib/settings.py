from typing import Optional

import attr

from bi_configs.environments import CommonInstallation
from bi_configs.crypto_keys import CryptoKeysConfig
from bi_configs.settings_loaders.meta_definition import required, s_attrib
from bi_configs.settings_submodels import RedisSettings, S3Settings


def _make_redis_persistent_settings(cfg: Optional[CommonInstallation], db: int) -> Optional[RedisSettings]:
    return RedisSettings(  # type: ignore
        MODE=cfg.REDIS_PERSISTENT_MODE,
        CLUSTER_NAME=cfg.REDIS_PERSISTENT_CLUSTER_NAME,
        HOSTS=cfg.REDIS_PERSISTENT_HOSTS,
        PORT=cfg.REDIS_PERSISTENT_PORT,
        SSL=cfg.REDIS_PERSISTENT_SSL,
        PASSWORD=required(str),

        DB=db,
    ) if isinstance(cfg, CommonInstallation) else None


@attr.s(frozen=True)
class FileUploaderBaseSettings:
    REDIS_APP: RedisSettings = s_attrib(  # type: ignore
        "REDIS_APP",
        fallback_factory=(
            lambda cfg: _make_redis_persistent_settings(cfg=cfg, db=CommonInstallation.REDIS_FILE_UPLOADER_DATA_DB)
        ),
    )

    REDIS_ARQ: RedisSettings = s_attrib(  # type: ignore
        "REDIS_ARQ",
        fallback_factory=(
            lambda cfg: _make_redis_persistent_settings(cfg=cfg, db=CommonInstallation.REDIS_FILE_UPLOADER_TASKS_DB)
        ),
    )

    S3: S3Settings = s_attrib(  # type: ignore
        "S3",
        fallback_factory=(
            lambda cfg: S3Settings(  # type: ignore
                ACCESS_KEY_ID=required(str),
                SECRET_ACCESS_KEY=required(str),
                ENDPOINT_URL=cfg.S3_ENDPOINT_URL,
            ) if isinstance(cfg, CommonInstallation) else None
        ),
    )
    S3_TMP_BUCKET_NAME: str = s_attrib(  # type: ignore
        "S3_TMP_BUCKET_NAME",
        fallback_cfg_key="FILE_UPLOADER_S3_TMP_BUCKET_NAME",
    )
    S3_PERSISTENT_BUCKET_NAME: str = s_attrib(  # type: ignore
        "S3_PERSISTENT_BUCKET_NAME",
        fallback_cfg_key="FILE_UPLOADER_S3_PERSISTENT_BUCKET_NAME",
    )

    CRYPTO_KEYS_CONFIG: CryptoKeysConfig = s_attrib(  # type: ignore
        "DL_CRY",
        json_converter=CryptoKeysConfig.from_json,
        sensitive=True,
    )
