from __future__ import annotations

from typing import TYPE_CHECKING, List, ClassVar

from bi_query_processing.legacy_pipeline.planning.planner import PrefilterAndCompengExecutionPlanner

from bi_constants.enums import BIType

import bi_formula.core.nodes as formula_nodes

if TYPE_CHECKING:
    from bi_query_processing.compilation.primitives import CompiledFormulaInfo, CompiledQuery


class BitrixGDSCompengExecutionPlanner(PrefilterAndCompengExecutionPlanner):
    expr_names: ClassVar[set[str]] = {'between', '>', '>=', '<', '<=', '=='}
    data_types: ClassVar[set[BIType]] = {BIType.datetime, BIType.date, BIType.genericdatetime}

    def _expr_is_prefilter(self, formula_info: CompiledFormulaInfo) -> bool:
        assert formula_info.original_field_id is not None
        expr = formula_info.formula_obj.expr
        if not isinstance(expr, formula_nodes.OperationCall):
            return False
        field = self.ds.result_schema.by_guid(formula_info.original_field_id)
        if field.data_type in self.data_types and expr.name in self.expr_names:
            return True
        return False

    def _get_needs_compeng(
            self, compiled_query: CompiledQuery, compeng_only_filters: List[CompiledFormulaInfo]
    ) -> bool:
        return True
