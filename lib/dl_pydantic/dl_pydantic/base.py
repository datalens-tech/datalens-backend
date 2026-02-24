import deepdiff
import pydantic
from typing_extensions import (
    Self,
    deprecated,
)

import dl_json


class BaseModel(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(
        populate_by_name=True,
        hide_input_in_errors=True,
    )

    def model_deepdiff(
        self,
        other: Self,
        exclude_paths: list[str] | None = None,
        exclude_regex_paths: list[str] | None = None,
    ) -> deepdiff.DeepDiff:
        exclude_paths = exclude_paths or []
        exclude_regex_paths = exclude_regex_paths or []

        exclude_regex_paths.extend([r"model_.*"])

        return deepdiff.DeepDiff(
            self,
            other,
            ignore_order=True,
            exclude_paths=exclude_paths,
            exclude_regex_paths=exclude_regex_paths,
        )


class BaseSchema(BaseModel):
    def model_dump_jsonable(
        self,
        *,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
    ) -> dl_json.JsonSerializableMapping:
        return self.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=exclude_none,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
        )

    @deprecated("Use model_dump_jsonable instead", category=DeprecationWarning)
    def model_get_body(
        self,
        *,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
    ) -> dl_json.JsonSerializableMapping:
        return self.model_dump_jsonable(
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
        )


__all__ = [
    "BaseModel",
    "BaseSchema",
]
