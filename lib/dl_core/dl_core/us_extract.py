import attr

from dl_constants.enums import (
    ExtractMode,
    ExtractStatus,
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
    sorting: list[OrderField] = attr.ib(default=attr.Factory(list))

    # unversioned
    status: ExtractStatus = attr.ib(default=ExtractStatus.disabled)
    errors: list[str] = attr.ib(factory=list)
    last_update: int = attr.ib(default=0)
