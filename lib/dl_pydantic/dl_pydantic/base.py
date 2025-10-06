import deepdiff
import pydantic
from typing_extensions import Self


class BaseModel(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(
        populate_by_name=True,
    )

    def model_deepdiff(self, other: Self, exclude_paths: list[str] | None = None) -> deepdiff.DeepDiff:
        exclude_paths = exclude_paths or []

        return deepdiff.DeepDiff(
            self,
            other,
            ignore_order=True,
            exclude_paths=exclude_paths,
        )


__all__ = [
    "BaseModel",
]
