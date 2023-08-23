from __future__ import annotations

from typing import TYPE_CHECKING, List

from bi_query_processing.legacy_pipeline.planning.planner import PrefilterAndCompengExecutionPlanner

if TYPE_CHECKING:
    from bi_query_processing.compilation.primitives import CompiledFormulaInfo, CompiledQuery


class GSheetsCompengExecutionPlanner(PrefilterAndCompengExecutionPlanner):

    def _expr_is_prefilter(self, formula_info: CompiledFormulaInfo) -> bool:
        # For starters, doing no prefilters. To be reconsidered later.
        return False

    def _get_needs_compeng(
            self, compiled_query: CompiledQuery, compeng_only_filters: List[CompiledFormulaInfo]
    ) -> bool:
        return not self._is_simple_query(
            compiled_query=compiled_query,
            compeng_only_filters=compeng_only_filters,
        )
