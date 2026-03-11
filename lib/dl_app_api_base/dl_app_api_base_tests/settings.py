import dl_settings


class Settings(dl_settings.BaseRootSettings):
    REWRITE_DOC_SPECS: bool = False


__all__ = [
    "Settings",
]
