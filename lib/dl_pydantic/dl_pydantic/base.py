import deepdiff
import pydantic
from typing_extensions import Self


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
    ...


__all__ = [
    "BaseModel",
    "BaseSchema",
]
