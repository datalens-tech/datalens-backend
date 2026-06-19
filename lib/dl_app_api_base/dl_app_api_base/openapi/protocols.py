from collections.abc import (
    Mapping,
    Sequence,
)
import http
from typing import Protocol

import dl_pydantic


class OpenApiHandlerProtocol(Protocol):
    @property
    def OPENAPI_TAGS(self) -> Sequence[str]: ...  # noqa: N802

    @property
    def OPENAPI_DESCRIPTION(self) -> str: ...  # noqa: N802

    @property
    def OPENAPI_INCLUDE(self) -> bool: ...  # noqa: N802

    @property
    def _request_schema(self) -> type[dl_pydantic.BaseModel]: ...

    @property
    def _response_schemas(self) -> Mapping[http.HTTPStatus, type[dl_pydantic.BaseModel]]: ...


class OpenApiRouteProtocol(Protocol):
    @property
    def path(self) -> str: ...

    @property
    def method(self) -> str: ...

    @property
    def handler(self) -> OpenApiHandlerProtocol: ...
