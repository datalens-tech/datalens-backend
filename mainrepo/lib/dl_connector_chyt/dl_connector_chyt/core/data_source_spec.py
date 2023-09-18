from __future__ import annotations

from typing import (
    Optional,
    TypeVar,
)

import attr

from dl_core.data_source_spec.sql import (
    SQLDataSourceSpecBase,
    SubselectDataSourceSpec,
    TableSQLDataSourceSpec,
)


class CHYTDataSourceSpecMixin(SQLDataSourceSpecBase):
    pass


class CHYTTableDataSourceSpec(CHYTDataSourceSpecMixin, TableSQLDataSourceSpec):
    pass


class CHYTSubselectDataSourceSpec(CHYTDataSourceSpecMixin, SubselectDataSourceSpec):
    pass


@attr.s
class CHYTTableListDataSourceSpec(CHYTDataSourceSpecMixin, SQLDataSourceSpecBase):
    # stored as a single `\n` separated string
    table_names: Optional[str] = attr.ib(kw_only=True, default=None)


_VAL_TV = TypeVar("_VAL_TV")


def _zero_to_none(value: _VAL_TV) -> Optional[_VAL_TV]:  # type: ignore
    return value or None


@attr.s
class CHYTTableRangeDataSourceSpec(CHYTDataSourceSpecMixin, SQLDataSourceSpecBase):
    directory_path: Optional[str] = attr.ib(kw_only=True, default=None)
    range_from: Optional[str] = attr.ib(kw_only=True, default=None, converter=_zero_to_none)  # type: ignore
    range_to: Optional[str] = attr.ib(kw_only=True, default=None, converter=_zero_to_none)  # type: ignore
