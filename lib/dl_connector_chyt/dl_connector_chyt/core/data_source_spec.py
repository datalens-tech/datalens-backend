from __future__ import annotations

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
    table_names: str | None = attr.ib(kw_only=True, default=None)


def _zero_to_none[VAL_TV](value: VAL_TV) -> VAL_TV | None:
    return value or None


@attr.s
class CHYTTableRangeDataSourceSpec(CHYTDataSourceSpecMixin, SQLDataSourceSpecBase):
    directory_path: str | None = attr.ib(kw_only=True, default=None)
    range_from: str | None = attr.ib(kw_only=True, default=None, converter=_zero_to_none)
    range_to: str | None = attr.ib(kw_only=True, default=None, converter=_zero_to_none)
