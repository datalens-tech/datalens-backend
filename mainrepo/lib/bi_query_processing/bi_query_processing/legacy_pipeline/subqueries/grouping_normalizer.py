from typing import (
    List,
    Sequence,
)

from bi_formula.inspect.env import InspectionEnvironment
from bi_formula.inspect.expression import (
    is_aggregate_expression,
    used_fields,
)
from bi_query_processing.compilation.primitives import (
    CompiledFormulaInfo,
    CompiledQuery,
)
from bi_query_processing.legacy_pipeline.separation.primitives import CompiledMultiLevelQuery
from bi_query_processing.legacy_pipeline.subqueries.query_tools import (
    CompiledMultiLevelQueryReplacementPatch,
    apply_multi_query_replacement_patches,
)


class GroupByNormalizer:
    def normalize_flat_query_grouping(
        self,
        compiled_flat_query: CompiledQuery,
    ) -> CompiledQuery:
        if not compiled_flat_query.group_by:
            # No GROUP BY, so nothing to fix
            return compiled_flat_query

        inspect_env = InspectionEnvironment()
        group_by_aliases = {formula.alias for formula in compiled_flat_query.group_by}
        add_group_by: List[CompiledFormulaInfo] = []
        for formula in compiled_flat_query.select:
            if formula.alias in group_by_aliases:
                # Already participates in GROUP BY, so skip
                continue

            expr = formula.formula_obj.expr
            if not is_aggregate_expression(expr, env=inspect_env) and used_fields(expr):
                # Not aggregated and field-dependent,
                # and, therefore, must be in GROUP BY
                add_group_by.append(formula)
                group_by_aliases.add(formula.alias)

        if add_group_by:
            compiled_flat_query = compiled_flat_query.clone(group_by=compiled_flat_query.group_by + add_group_by)

        return compiled_flat_query

    def _gen_patches_for_multi_query(
        self,
        compiled_multi_query: CompiledMultiLevelQuery,
    ) -> Sequence[CompiledMultiLevelQueryReplacementPatch]:
        patches: List[CompiledMultiLevelQueryReplacementPatch] = []
        for subquery_idx, compiled_flat_query in compiled_multi_query.enumerate():
            updated_query = self.normalize_flat_query_grouping(compiled_flat_query=compiled_flat_query)
            if updated_query is not compiled_flat_query:
                patches.append(
                    CompiledMultiLevelQueryReplacementPatch(
                        subquery_idx=subquery_idx,
                        new_subquery=updated_query,
                    )
                )

        return patches

    def normalize_multi_query_grouping(
        self,
        compiled_multi_query: CompiledMultiLevelQuery,
    ) -> CompiledMultiLevelQuery:
        replacement_patches = self._gen_patches_for_multi_query(compiled_multi_query=compiled_multi_query)
        compiled_multi_query = apply_multi_query_replacement_patches(
            compiled_multi_query=compiled_multi_query,
            patches=replacement_patches,
        )
        return compiled_multi_query
