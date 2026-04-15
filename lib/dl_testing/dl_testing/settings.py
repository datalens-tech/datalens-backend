from typing import ClassVar

import dl_settings


class BaseRootSettings(dl_settings.BaseRootSettings):
    MODEL_ENABLE_EXTRA_FIELDS_WARNING: ClassVar[bool] = False


__all__ = [
    "BaseRootSettings",
]
