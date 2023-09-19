from __future__ import annotations

from typing import Union

import attr

from dl_constants.types import TBIDataValue


CellValue = Union[TBIDataValue, dict]


@attr.s(frozen=True)
class DimensionSpec:
    legend_item_id: int = attr.ib(kw_only=True)


@attr.s(frozen=True)
class DimensionValueSpec:
    legend_item_id: int = attr.ib(kw_only=True)
    value: CellValue = attr.ib(kw_only=True)
