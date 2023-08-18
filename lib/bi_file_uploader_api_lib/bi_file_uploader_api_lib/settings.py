from typing import Optional

import json
import attr

from bi_configs.settings_loaders.meta_definition import s_attrib, required
from bi_configs.settings_submodels import (
    CorsSettings,
    CsrfSettings,
    YCAuthSettings,
    default_yc_auth_settings,
)
from bi_configs.environments import CSRFAwareInstallation, CORSAwareInstallation

from bi_file_uploader_lib.settings import FileUploaderBaseSettings


@attr.s(frozen=True)
class FileUploaderAPISettings(FileUploaderBaseSettings):
    CORS: CorsSettings = s_attrib(  # type: ignore
        "CORS",
        fallback_factory=(
            lambda cfg: CorsSettings(  # type: ignore
                ALLOWED_ORIGINS=cfg.CORS_ALLOWED_ORIGINS,
                ALLOWED_HEADERS=cfg.CORS_ALLOWED_HEADERS,
                EXPOSE_HEADERS=cfg.CORS_EXPOSE_HEADERS,
            ) if isinstance(cfg, CORSAwareInstallation) else None
        ),
    )

    CSRF: CsrfSettings = s_attrib(  # type: ignore
        "CSRF",
        fallback_factory=(
            lambda cfg: CsrfSettings(  # type: ignore
                METHODS=cfg.CSRF_METHODS,
                HEADER_NAME=cfg.CSRF_HEADER_NAME,
                TIME_LIMIT=cfg.CSRF_TIME_LIMIT,
                SECRET=required(str),
            ) if isinstance(cfg, CSRFAwareInstallation) else None
        ),
    )

    SENTRY_DSN: Optional[str] = s_attrib(  # type: ignore
        "SENTRY_DSN", fallback_cfg_key="SENTRY_DSN_FILE_UPLOADER_API", missing=None,
    )

    YC_AUTH_SETTINGS: Optional[YCAuthSettings] = s_attrib("YC_AUTH_SETTINGS", fallback_factory=default_yc_auth_settings)  # type: ignore
    SA_KEY_DATA: Optional[dict[str, str]] = s_attrib(  # type: ignore
        "SA_KEY_DATA", sensitive=True, missing=None, env_var_converter=json.loads,
    )

    FILE_UPLOADER_MASTER_TOKEN: str = s_attrib("FILE_UPLOADER_MASTER_TOKEN", sensitive=True)  # type: ignore

    ALLOW_XLSX: bool = s_attrib("ALLOW_XLSX", missing=False)  # type: ignore
