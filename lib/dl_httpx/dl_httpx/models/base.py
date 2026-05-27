import logging
from typing import (
    Any,
    Protocol,
)

import attrs
import pydantic
from typing_extensions import Self

import dl_auth
import dl_constants
import dl_json
import dl_pydantic
import dl_utils

LOGGER = logging.getLogger(__name__)


class ParentContextProtocol(Protocol):
    @property
    def request_id(self) -> str | None: ...

    @property
    def user_ip(self) -> str | None: ...

    @property
    def trace_id(self) -> str | None: ...


@attrs.define(kw_only=True, frozen=True)
class ParentContext:
    request_id: str | None = None
    user_ip: str | None = None
    trace_id: str | None = None


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
        result: dict[str, str] = {}
        result[dl_constants.DLHeadersCommon.REQUEST_ID.value] = self.request_id
        if self.parent_context.user_ip is not None:
            result[dl_constants.DLHeadersCommon.REAL_IP.value] = self.parent_context.user_ip
        if self.parent_context.trace_id is not None:
            result[dl_constants.DLHeadersCommon.UBER_TRACE_ID.value] = self.parent_context.trace_id

        return result

    @property
    def cookies(self) -> dict[str, str]:
        return {}


_MAX_LOGGED_RAW_PAYLOAD_CHARS = 5000


def _truncate_for_log(value: Any) -> str:
    rendered = repr(value)
    if len(rendered) <= _MAX_LOGGED_RAW_PAYLOAD_CHARS:
        return rendered
    return f"{rendered[:_MAX_LOGGED_RAW_PAYLOAD_CHARS]}... [truncated, {len(rendered)} chars total]"


class BaseResponseSchema(dl_pydantic.BaseSchema):
    @classmethod
    def model_validate(cls, obj: Any, *args: Any, **kwargs: Any) -> Self:
        try:
            return super().model_validate(obj, *args, **kwargs)
        except pydantic.ValidationError:
            LOGGER.error(
                "Response validation failed for %s, raw data: %s",
                cls.__name__,
                _truncate_for_log(obj),
            )
            raise

    @classmethod
    def model_validate_json(cls, json_data: str | bytes | bytearray, *args: Any, **kwargs: Any) -> Self:
        try:
            return super().model_validate_json(json_data, *args, **kwargs)
        except pydantic.ValidationError:
            LOGGER.error(
                "Response validation failed for %s, raw data: %s",
                cls.__name__,
                _truncate_for_log(json_data),
            )
            raise


__all__ = [
    "BaseRequest",
    "BaseResponseSchema",
    "ParentContext",
    "ParentContextProtocol",
]
