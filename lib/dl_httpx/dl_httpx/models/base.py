from typing import Protocol

import attrs

import dl_auth
import dl_constants
import dl_json
import dl_pydantic
import dl_utils


class ParentContextProtocol(Protocol):
    @property
    def request_id(self) -> str | None:
        ...


@attrs.define(kw_only=True, frozen=True)
class ParentContext:
    request_id: str | None = None


@attrs.define(kw_only=True, frozen=True)
class BaseRequest:
    parent_context: ParentContextProtocol = attrs.field(factory=ParentContext)
    auth_provider: dl_auth.AuthProviderProtocol | None = None
    request_id: str = attrs.field()

    @request_id.default
    def _generate_request_id(self) -> str:
        current = dl_utils.request_id_generator()
        if self.parent_context.request_id is None:
            return current

        return dl_utils.make_uuid_from_parts(
            current=current,
            parent=self.parent_context.request_id,
        )

    @property
    def path(self) -> str:
        raise NotImplementedError

    @property
    def method(self) -> str:
        raise NotImplementedError

    @property
    def query_params(self) -> dict[str, str]:
        return {}

    @property
    def body(self) -> dl_json.JsonSerializableMapping | None:
        return None

    @property
    def headers(self) -> dict[str, str]:
        return {
            dl_constants.DLHeadersCommon.REQUEST_ID.value: self.request_id,
        }

    @property
    def cookies(self) -> dict[str, str]:
        return {}


# Deprecated, use dl_pydantic instead
BaseSchema = dl_pydantic.BaseSchema


class BaseResponseSchema(dl_pydantic.BaseSchema):
    ...


__all__ = [
    "BaseRequest",
    "BaseResponseSchema",
    "BaseSchema",
    "ParentContext",
    "ParentContextProtocol",
]
