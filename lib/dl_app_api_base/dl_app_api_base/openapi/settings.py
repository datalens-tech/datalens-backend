import typing_extensions

import dl_settings


class OpenApiSettings(dl_settings.BaseSettings):
    DOCS_PATH: str = "/api/v1/docs"
    SPEC_REL_URL: str = "/spec.json"
    SWAGGER_UI_ENABLED: bool = True
    EXTERNAL_ROUTE_PREFIX: str = ""

    @property
    @typing_extensions.deprecated("Use SPEC_PATH instead", category=DeprecationWarning)
    def spec_path(self) -> str:
        return self.SPEC_PATH

    @property
    def SPEC_PATH(self) -> str:
        return f"{self.DOCS_PATH}{self.SPEC_REL_URL}"

    @property
    def EXTERNAL_DOCS_PATH(self) -> str:
        return f"{self.EXTERNAL_ROUTE_PREFIX}{self.DOCS_PATH}"
