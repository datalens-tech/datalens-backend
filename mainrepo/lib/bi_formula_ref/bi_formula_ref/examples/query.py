from typing import Optional, Sequence

import attr
from sqlalchemy.sql.selectable import Select

import bi_formula.core.nodes as nodes

from bi_formula_ref.examples.data_table import DataColumn


@attr.s
class TableReference:
    name: str = attr.ib(kw_only=True)
    columns: Sequence[DataColumn] = attr.ib(kw_only=True)


@attr.s
class FormulaContext:
    formula: nodes.Formula = attr.ib(kw_only=True)
    alias: Optional[str] = attr.ib(kw_only=True)


@attr.s
class RawQueryContext:
    result_columns: Sequence[DataColumn] = attr.ib(kw_only=True)
    select: Sequence[FormulaContext] = attr.ib(kw_only=True)
    group_by: Sequence[FormulaContext] = attr.ib(kw_only=True, default=())
    order_by: Sequence[FormulaContext] = attr.ib(kw_only=True, default=())


@attr.s
class CompiledQueryContext:
    result_columns: Sequence[DataColumn] = attr.ib(kw_only=True)
    sa_query: Select = attr.ib(kw_only=True)
