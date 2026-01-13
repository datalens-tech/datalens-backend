import http
from typing import (
    ClassVar,
    Protocol,
    runtime_checkable,
)

import dl_app_api_base.handlers.base as handlers_base


@runtime_checkable
class OpenApiHandlerProtocol(Protocol):
    OPENAPI_TAGS: ClassVar[list[str]]
    OPENAPI_DESCRIPTION: ClassVar[str]
    OPENAPI_INCLUDE: ClassVar[bool]

    RequestSchema: ClassVar[type[handlers_base.BaseRequestSchema]]

    @property
    def _response_schemas(self) -> dict[http.HTTPStatus, type[handlers_base.BaseResponseSchema]]:
        ...


@runtime_checkable
class OpenApiRouteProtocol(Protocol):
    path: str
    method: str
    handler: OpenApiHandlerProtocol
