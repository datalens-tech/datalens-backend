from typing import Optional

import attr

from bi_configs.environments import CommonInstallation
from bi_configs.settings_loaders.meta_definition import s_attrib, required
from bi_configs.connectors_settings import ConnectorsSettingsByType, connectors_settings_file_only_fallback_factory
from bi_configs.settings_submodels import GoogleAppSettings

from bi_file_uploader_lib.settings import FileUploaderBaseSettings


@attr.s(frozen=True)
class FileUploaderWorkerSettings(FileUploaderBaseSettings):
    SENTRY_DSN: Optional[str] = s_attrib(  # type: ignore
        "SENTRY_DSN", fallback_cfg_key="SENTRY_DSN_FILE_UPLOADER_WORKER", missing=None,
    )

    US_BASE_URL: str = s_attrib("US_HOST", fallback_cfg_key="US_BASE_URL")  # type: ignore
    US_MASTER_TOKEN: str = s_attrib("US_MASTER_TOKEN", sensitive=True)  # type: ignore

    CONNECTORS: Optional[ConnectorsSettingsByType] = s_attrib(  # type: ignore
        "CONNECTORS",
        fallback_factory=connectors_settings_file_only_fallback_factory,
    )

    GSHEETS_APP: GoogleAppSettings = s_attrib(  # type: ignore
        "GSHEETS_APP",
        fallback_factory=(
            lambda cfg: GoogleAppSettings(  # type: ignore
                API_KEY=required(str),
                CLIENT_ID=required(str),
                CLIENT_SECRET=required(str),
            ) if isinstance(cfg, CommonInstallation) else None
        ),
    )

    ENABLE_REGULAR_S3_LC_RULES_CLEANUP: bool = s_attrib("ENABLE_REGULAR_S3_LC_RULES_CLEANUP", missing=False)  # type: ignore
    SECURE_READER_SOCKET: str = s_attrib("SECURE_READER_SOCKET", missing="/var/reader.sock")  # type: ignore
