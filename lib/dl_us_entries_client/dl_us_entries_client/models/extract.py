import pydantic

from dl_constants import (
    ExtractMode,
    ExtractStatus,
)
import dl_pydantic


class ExtractProperties(dl_pydantic.BaseModel):
    mode: ExtractMode = pydantic.Field(default=ExtractMode.disabled)


class UnversionedExtractProperties(dl_pydantic.BaseModel):
    status: ExtractStatus = pydantic.Field(default=ExtractStatus.disabled)
    errors: list[str] = pydantic.Field(default_factory=list)
    last_completed: int = pydantic.Field(default=0)
    data_dataset_revision: str | None = None
