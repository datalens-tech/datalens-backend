from typing import Any

import attrs

import dl_pydantic


@attrs.define(kw_only=True, frozen=True)
class BaseRequest:
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


class BaseSchema(dl_pydantic.BaseModel):
    def model_get_body(self) -> dict[str, Any]:
        return self.model_dump(mode="json", by_alias=True)


class BaseResponseSchema(BaseSchema):
    ...


__all__ = [
    "BaseResponseSchema",
    "BaseSchema",
    "BaseRequest",
]
