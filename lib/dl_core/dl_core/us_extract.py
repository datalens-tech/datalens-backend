import attr

from dl_constants import (
    ExtractMode,
    ExtractStatus,
    TopLevelComponentId,
)
from dl_core.fields import (
    FilterField,
    OrderField,
)


@attr.s
class ExtractProperties:
    # versioned
    mode: ExtractMode = attr.ib(default=ExtractMode.disabled)
    filters: list[FilterField] = attr.ib(factory=list)
    sorting: list[OrderField] = attr.ib(factory=list)

    # unversioned
    status: ExtractStatus = attr.ib(default=ExtractStatus.disabled)
    errors: list[str] = attr.ib(factory=list)
    last_completed: int = attr.ib(default=0)
    data_dataset_revision: str | None = attr.ib(default=None)

    # validation status
    valid: bool = attr.ib(default=True)

    @property
    def id(self) -> str:
        return TopLevelComponentId.extract_properties.value
