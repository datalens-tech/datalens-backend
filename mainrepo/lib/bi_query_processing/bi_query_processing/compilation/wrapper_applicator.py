from __future__ import annotations

from typing import Sequence

import bi_formula.core.nodes as formula_nodes

from bi_query_processing.enums import SelectValueType
from bi_query_processing.compilation.specs import SelectWrapperSpec, ArrayPrefixSelectWrapperSpec


class ExpressionWrapperApplicator:
    def _apply_func_wrapper(
            self, expr: formula_nodes.FormulaItem, func_name: str,
            additional_args: Sequence[formula_nodes.FormulaItem] = (),
    ) -> formula_nodes.FormulaItem:
        func_name = func_name.lower()
        return formula_nodes.FuncCall.make(
            name=func_name,
            args=[expr, *additional_args],
        )

    def _apply_wrapper_min(self, expr: formula_nodes.FormulaItem) -> formula_nodes.FormulaItem:
        return self._apply_func_wrapper(expr, func_name='min')

    def _apply_wrapper_max(self, expr: formula_nodes.FormulaItem) -> formula_nodes.FormulaItem:
        return self._apply_func_wrapper(expr, func_name='max')

    def _apply_wrapper_array_prefix(
            self, expr: formula_nodes.FormulaItem, length: int,
    ) -> formula_nodes.FormulaItem:
        return self._apply_func_wrapper(
            expr, func_name='slice',
            additional_args=[
                formula_nodes.LiteralInteger.make(1),
                formula_nodes.LiteralInteger.make(length),
            ],
        )

    def apply_wrapper(self, expr: formula_nodes.FormulaItem, wrapper: SelectWrapperSpec) -> formula_nodes.FormulaItem:
        if wrapper.type == SelectValueType.plain:
            pass
        elif wrapper.type == SelectValueType.min:
            expr = self._apply_wrapper_min(expr)
        elif wrapper.type == SelectValueType.max:
            expr = self._apply_wrapper_max(expr)
        elif wrapper.type == SelectValueType.array_prefix:
            assert isinstance(wrapper, ArrayPrefixSelectWrapperSpec)
            expr = self._apply_wrapper_array_prefix(expr, length=wrapper.length)
        else:
            raise ValueError(f'Unsupported wrapper.type value: {wrapper.type}')

        return expr
