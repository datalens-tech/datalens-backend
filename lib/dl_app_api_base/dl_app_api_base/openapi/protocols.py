import http
from typing import Protocol

import dl_app_api_base.handlers.base as handlers_base


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
    def RequestSchema(self) -> type[handlers_base.BaseRequestSchema]:
        ...

    @property
    def _response_schemas(self) -> dict[http.HTTPStatus, type[handlers_base.BaseResponseSchema]]:
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
