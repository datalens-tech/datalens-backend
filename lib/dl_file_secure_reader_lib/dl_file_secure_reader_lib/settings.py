import pydantic

import dl_settings


class FileSecureReaderSettings(
    dl_settings.BaseSettings,
):
    FEATURE_EXCEL_READ_ONLY: bool = True


class FileSecureReaderAppSettings(
    dl_settings.BaseRootSettings,
):
    FILE_SECURE_READER_SETTINGS: FileSecureReaderSettings = pydantic.Field(default_factory=FileSecureReaderSettings)
