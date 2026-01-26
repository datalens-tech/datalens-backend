import http
from typing import (
    Mapping,
    Protocol,
)

import dl_pydantic


class OpenApiHandlerProtocol(Protocol):
    @property
    def OPENAPI_TAGS(self) -> list[str]:
        ...

    @property
    def OPENAPI_DESCRIPTION(self) -> str:
        ...

    @property
    def OPENAPI_INCLUDE(self) -> bool:
        ...

    @property
    def _request_schema(self) -> type[dl_pydantic.BaseModel]:
        ...

    @property
    def _response_schemas(self) -> Mapping[http.HTTPStatus, type[dl_pydantic.BaseModel]]:
        ...


class OpenApiRouteProtocol(Protocol):
    @property
    def path(self) -> str:
        ...

    @property
    def method(self) -> str:
        ...

    @property
    def handler(self) -> OpenApiHandlerProtocol:
        ...
