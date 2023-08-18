from __future__ import annotations

from typing import List, Optional, TYPE_CHECKING

import attr

from bi_formula.inspect.env import InspectionEnvironment
from bi_formula.inspect.expression import infer_data_type
from bi_formula.parser.factory import get_parser, ParserType

from bi_formula_ref.examples.data_table import DataColumn
from bi_formula_ref.examples.config import ExampleConfig
from bi_formula_ref.examples.query import RawQueryContext, FormulaContext

if TYPE_CHECKING:
    from bi_formula.parser.base import FormulaParser


@attr.s
class QueryGenerator:
    _parser_type: ParserType = attr.ib(kw_only=True, default=ParserType.antlr_py)
    _parser: FormulaParser = attr.ib(init=False)

    def __attrs_post_init__(self) -> None:
        self._parser = get_parser(self._parser_type)

    def _make_formula_ctx(self, formula: str, alias: Optional[str] = None) -> FormulaContext:
        formula_obj = self._parser.parse(formula)
        return FormulaContext(formula=formula_obj, alias=alias)

    def generate_query(self, example: ExampleConfig) -> RawQueryContext:
        inspect_env = InspectionEnvironment()

        columns = [DataColumn(name=name, data_type=data_type) for name, data_type in example.source.columns]

        all_formulas = {alias: formula for formula, alias in example.formula_fields}
        all_formulas.update({col.name: f'[{col.name}]' for col in columns})

        col_data_types = {col.name: col.data_type for col in columns}

        select = [self._make_formula_ctx(formula, alias) for alias, formula in example.formula_fields]
        group_by = [self._make_formula_ctx(formula) for formula in example.group_by]
        order_by = [self._make_formula_ctx(formula) for formula in example.order_by]
        result_columns: List[DataColumn] = []
        for ctx in select:
            alias = ctx.alias
            assert alias is not None
            result_columns.append(DataColumn(
                name=alias,
                data_type=infer_data_type(
                    node=ctx.formula, field_types=col_data_types, env=inspect_env,
                ),
            ))

        query_ctx = RawQueryContext(
            result_columns=result_columns,
            select=select, group_by=group_by, order_by=order_by,
        )
        return query_ctx
