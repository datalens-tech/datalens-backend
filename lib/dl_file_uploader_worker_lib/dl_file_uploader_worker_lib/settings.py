from typing import Optional

import attr
import pydantic

from dl_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from dl_configs.settings_loaders.meta_definition import s_attrib
from dl_configs.settings_loaders.settings_obj_base import SettingsBase
from dl_configs.settings_submodels import GoogleAppSettings
from dl_configs.utils import (
    get_root_certificates_path,
    split_by_comma,
)
from dl_core.us_manager.settings import USClientSettings
from dl_file_secure_reader_lib.settings import FileSecureReaderSettings
from dl_file_uploader_lib.settings import (
    DeprecatedFileUploaderBaseSettings,
    FileUploaderBaseSettings,
)

from dl_connector_bundle_chs3.chs3_base.core.settings import FileS3ConnectorSettings
from dl_connector_bundle_chs3.file.core.settings import file_s3_settings_fallback


@attr.s(frozen=True)
class SecureReader(SettingsBase):
    SOCKET: str = s_attrib("SOCKET")  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str")  [assignment]
    ENDPOINT: Optional[str] = s_attrib("ENDPOINT", missing=None)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str | None")  [assignment]
    CAFILE: Optional[str] = s_attrib("CAFILE", missing=None)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str | None")  [assignment]


@attr.s(frozen=True)
class FileUploaderConnectorsSettings(SettingsBase):
    FILE: Optional[FileS3ConnectorSettings] = s_attrib("FILE", missing=None)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "FileS3ConnectorSettings | None")  [assignment]


def file_uploader_connectors_settings_fallback(full_cfg: ObjectLikeConfig) -> FileUploaderConnectorsSettings:
    settings = file_s3_settings_fallback(full_cfg)
    return FileUploaderConnectorsSettings(**settings)


@attr.s(frozen=True)
class DeprecatedFileUploaderWorkerSettings(DeprecatedFileUploaderBaseSettings):
    MAX_CONCURRENT_JOBS: int = s_attrib("MAX_CONCURRENT_JOBS", missing=15)  # type: ignore

    SENTRY_DSN: Optional[str] = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str | None")  [assignment]
        "DL_SENTRY_DSN",
        fallback_cfg_key="SENTRY_DSN_FILE_UPLOADER_WORKER",
        missing=None,
    )

    US_BASE_URL: str = s_attrib("US_HOST", fallback_cfg_key="US_BASE_URL")  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str")  [assignment]
    US_MASTER_TOKEN: str = s_attrib("US_MASTER_TOKEN", sensitive=True)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str")  [assignment]

    CONNECTORS: FileUploaderConnectorsSettings = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "FileUploaderConnectorsSettings")  [assignment]
        "CONNECTORS",
        fallback_factory=file_uploader_connectors_settings_fallback,
    )

    GSHEETS_APP: GoogleAppSettings = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "GoogleAppSettings")  [assignment]
        # TODO: check gsheets connector availability
        "GSHEETS_APP",
    )

    ENABLE_REGULAR_S3_LC_RULES_CLEANUP: bool = s_attrib("ENABLE_REGULAR_S3_LC_RULES_CLEANUP", missing=False)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "bool")  [assignment]
    SECURE_READER: SecureReader = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "SecureReader")  [assignment]
        "SECURE_READER",
        fallback_factory=(
            lambda: SecureReader(  # type: ignore  # 2024-01-30 # TODO: Argument "fallback_factory" to "s_attrib" has incompatible type "Callable[[], SecureReader]"; expected "Callable[[Any, Any], Any] | Callable[[Any], Any] | None"  [arg-type]
                SOCKET="/var/reader.sock",
                ENDPOINT=None,
                CAFILE=None,
            )
        ),
    )

    CA_FILE_PATH: str = s_attrib("CA_FILE_PATH", missing=get_root_certificates_path())  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str")  [assignment]
    EXTRA_CA_FILE_PATHS: tuple[str, ...] = s_attrib(  # type: ignore # 2024-07-29 # TODO
        "EXTRA_CA_FILE_PATHS",
        env_var_converter=split_by_comma,
        missing_factory=tuple,
    )


class FileUploaderWorkerSettings(FileUploaderBaseSettings):
    US_CLIENT: USClientSettings = pydantic.Field(default_factory=USClientSettings)
    FILE_SECURE_READER_SETTINGS: FileSecureReaderSettings = pydantic.Field(default_factory=FileSecureReaderSettings)
