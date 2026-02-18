from typing import (
    Any,
    Protocol,
)

import attrs

import dl_constants
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
    request_id: str = attrs.field()

    @request_id.default
    def _generate_request_id(self) -> str:
        current = dl_utils.request_id_generator()
        if self.parent_context.request_id is not None:
            return dl_utils.make_uuid_from_parts(
                current=current,
                parent=self.parent_context.request_id,
            )
        return current

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
    def body(self) -> dict[str, Any] | None:
        return None

    @property
    def headers(self) -> dict[str, str]:
        return {
            dl_constants.DLHeadersCommon.REQUEST_ID.value: self.request_id,
        }

    @property
    def cookies(self) -> dict[str, str]:
        return {}


class BaseSchema(dl_pydantic.BaseSchema):
    def model_get_body(
        self,
        *,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
    ) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=exclude_none,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
        )


class BaseResponseSchema(BaseSchema):
    ...


__all__ = [
    "BaseRequest",
    "BaseResponseSchema",
    "BaseSchema",
    "ParentContext",
    "ParentContextProtocol",
]
