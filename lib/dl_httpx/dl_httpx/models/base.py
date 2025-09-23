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


class BaseResponseModel(dl_pydantic.BaseModel):
    ...


__all__ = [
    "BaseResponseModel",
    "BaseRequest",
]
