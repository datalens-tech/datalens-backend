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
from dl_configs.utils import (
    get_root_certificates_path,
    split_by_comma,
)
from dl_file_uploader_lib.settings import FileUploaderBaseSettings


@attr.s(frozen=True)
class FileUploaderAPISettings(FileUploaderBaseSettings):
    CORS: CorsSettings = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "CorsSettings")  [assignment]
        "CORS",
        fallback_factory=(
            lambda cfg: CorsSettings(  # type: ignore  # 2024-01-30 # TODO: Unexpected keyword argument "ALLOWED_ORIGINS" for "CorsSettings"  [call-arg]
                ALLOWED_ORIGINS=cfg.CORS_ALLOWED_ORIGINS,
                ALLOWED_HEADERS=cfg.CORS_ALLOWED_HEADERS,
                EXPOSE_HEADERS=cfg.CORS_EXPOSE_HEADERS,
                # TODO: move this values to a separate key
            )
            if is_setting_applicable(cfg, "CORS_ALLOWED_ORIGINS")
            else None
        ),
    )

    CSRF: CsrfSettings = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "CsrfSettings")  [assignment]
        "CSRF",
        fallback_factory=(
            lambda cfg: CsrfSettings(  # type: ignore  # 2024-01-30 # TODO: Unexpected keyword argument "METHODS" for "CsrfSettings"  [call-arg]
                METHODS=cfg.CSRF_METHODS,
                HEADER_NAME=cfg.CSRF_HEADER_NAME,
                TIME_LIMIT=cfg.CSRF_TIME_LIMIT,
                SECRET=required(tuple[str, ...]),
                # TODO: move this values to a separate key
            )
            if is_setting_applicable(cfg, "CSRF_METHODS")
            else None
        ),
    )

    SENTRY_DSN: Optional[str] = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str | None")  [assignment]
        "SENTRY_DSN",
        fallback_cfg_key="SENTRY_DSN_FILE_UPLOADER_API",
        missing=None,
    )

    FILE_UPLOADER_MASTER_TOKEN: str = s_attrib("FILE_UPLOADER_MASTER_TOKEN", sensitive=True)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str")  [assignment]

    ALLOW_XLSX: bool = s_attrib("ALLOW_XLSX", missing=False)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "bool")  [assignment]

    CA_FILE_PATH: str = s_attrib("CA_FILE_PATH", missing=get_root_certificates_path())  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str")  [assignment]
    EXTRA_CA_FILE_PATHS: tuple[str, ...] = s_attrib(  # type: ignore # 2024-07-29 # TODO
        "EXTRA_CA_FILE_PATHS",
        env_var_converter=split_by_comma,
        missing_factory=tuple,
    )
