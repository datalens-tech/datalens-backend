from typing import Optional

import attr

from dl_configs.environments import is_setting_applicable
from dl_configs.settings_loaders.meta_definition import (
    required,
    s_attrib,
)
from dl_configs.settings_submodels import (
    CorsSettings,
    CsrfSettings,
)
from dl_configs.utils import get_root_certificates_path
from dl_file_uploader_lib.settings import FileUploaderBaseSettings


@attr.s(frozen=True)
class FileUploaderAPISettings(FileUploaderBaseSettings):
    CORS: CorsSettings = s_attrib(
        "CORS",
        fallback_factory=(
            lambda cfg: CorsSettings(
                ALLOWED_ORIGINS=cfg.CORS_ALLOWED_ORIGINS,
                ALLOWED_HEADERS=cfg.CORS_ALLOWED_HEADERS,
                EXPOSE_HEADERS=cfg.CORS_EXPOSE_HEADERS,
                # TODO: move this values to a separate key
            )
            if is_setting_applicable(cfg, "CORS_ALLOWED_ORIGINS")
            else None
        ),
    )

    CSRF: CsrfSettings = s_attrib(
        "CSRF",
        fallback_factory=(
            lambda cfg: CsrfSettings(
                METHODS=cfg.CSRF_METHODS,
                HEADER_NAME=cfg.CSRF_HEADER_NAME,
                TIME_LIMIT=cfg.CSRF_TIME_LIMIT,
                SECRET=required(str),
                # TODO: move this values to a separate key
            )
            if is_setting_applicable(cfg, "CSRF_METHODS")
            else None
        ),
    )

    SENTRY_DSN: Optional[str] = s_attrib(
        "SENTRY_DSN",
        fallback_cfg_key="SENTRY_DSN_FILE_UPLOADER_API",
        missing=None,
    )

    FILE_UPLOADER_MASTER_TOKEN: str = s_attrib("FILE_UPLOADER_MASTER_TOKEN", sensitive=True)

    ALLOW_XLSX: bool = s_attrib("ALLOW_XLSX", missing=False)

    CA_FILE_PATH: str = s_attrib("CA_FILE_PATH", missing=get_root_certificates_path())  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str")  [assignment]
