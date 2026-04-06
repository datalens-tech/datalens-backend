import dl_settings


class Settings(dl_settings.BaseRootSettings):
    MODEL_ENABLE_EXTRA_FIELDS_WARNING = False

    REWRITE_DOC_SPECS: bool = False


__all__ = [
    "Settings",
]
