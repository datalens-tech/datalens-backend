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
    "BaseResponseSchema",
    "BaseSchema",
    "BaseRequest",
]
