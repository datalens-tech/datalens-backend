from itertools import chain
from typing import (
    Any,
    Dict,
    List,
)

import attr

from dl_formula.core.datatype import DataType


@attr.s(frozen=True, slots=True)
class DataColumn:
    name: str = attr.ib(kw_only=True)
    data_type: DataType = attr.ib(kw_only=True)


@attr.s(frozen=True, slots=True)
class DataTable:
    columns: List[DataColumn] = attr.ib(kw_only=True)
    rows: List[List[Any]] = attr.ib(kw_only=True)


def horizontal_join(*tables: DataTable) -> DataTable:
    lengths = {len(t.rows) for t in tables}
    assert len(lengths) == 1, f"All tables must have the same number of rows for horizontal join, got {lengths}"
    return DataTable(
        columns=[col for t in tables for col in t.columns],
        rows=[list(chain.from_iterable(concat_row_parts)) for concat_row_parts in zip(*(t.rows for t in tables))],
    )


def rename_columns(table: DataTable, name_mapping: Dict[str, str]) -> DataTable:
    return DataTable(
        columns=[DataColumn(name=name_mapping[col.name], data_type=col.data_type) for col in table.columns],
        rows=table.rows,
    )
