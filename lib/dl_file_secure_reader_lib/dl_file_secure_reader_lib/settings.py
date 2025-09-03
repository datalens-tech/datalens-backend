import pydantic

import dl_settings


class FileSecureReaderSettings(
    dl_settings.BaseRootSettings,
):
    FEATURE_EXCEL_READ_ONLY: bool = pydantic.Field(default=False)
