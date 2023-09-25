from __future__ import annotations

from itertools import chain
from itertools import count as it_count
from typing import (
    Optional,
    TypeVar,
)

import attr

from dl_constants.enums import OrderDirection
from dl_core.components.accessor import DatasetComponentAccessor
from dl_core.components.ids import (
    FieldId,
    RelationId,
)
from dl_core.us_dataset import Dataset
from dl_core.utils import attrs_evolve_to_subclass
from dl_formula.collections import (
    NodeSet,
    NodeValueMap,
)
import dl_formula.core.nodes as formula_nodes
from dl_query_processing.column_registry import ColumnRegistry
from dl_query_processing.compilation.base import RawQueryCompilerBase
from dl_query_processing.compilation.filter_compiler import FilterFormulaCompiler
from dl_query_processing.compilation.formula_compiler import FormulaCompiler
from dl_query_processing.compilation.helpers import make_joined_from_for_avatars
from dl_query_processing.compilation.primitives import (
    BASE_QUERY_ID,
    CompiledFormulaInfo,
    CompiledJoinOnFormulaInfo,
    CompiledOrderByFormulaInfo,
    CompiledQuery,
)
from dl_query_processing.compilation.specs import (
    QuerySpec,
    SelectWrapperSpec,
)
from dl_query_processing.compilation.wrapper_applicator import ExpressionWrapperApplicator
from dl_query_processing.enums import (
    ExecutionLevel,
    SelectValueType,
)


_COMPILED_FORMULA_INFO_TV = TypeVar("_COMPILED_FORMULA_INFO_TV", bound=CompiledFormulaInfo)


@attr.s
class DefaultQueryCompiler(RawQueryCompilerBase):
    _dataset: Dataset = attr.ib(kw_only=True)
    _column_reg: ColumnRegistry = attr.ib(kw_only=False)
    _formula_compiler: FormulaCompiler = attr.ib(kw_only=False)
    _filter_compiler: FilterFormulaCompiler = attr.ib(kw_only=True)
    _wrapper_applicator: ExpressionWrapperApplicator = attr.ib(kw_only=True, factory=ExpressionWrapperApplicator)

    _alias_cnt: it_count = attr.ib(init=False, factory=it_count)  # type: ignore  # TODO: fix
    _alias_node_mapping: NodeValueMap[str] = attr.ib(init=False, factory=NodeValueMap)
    _ds_accessor: DatasetComponentAccessor = attr.ib(init=False)

    @_ds_accessor.default
    def _make_ds_accessor(self) -> DatasetComponentAccessor:
        return DatasetComponentAccessor(dataset=self._dataset)

    def _get_expression_alias(self, expr: formula_nodes.Formula, force: bool = False) -> str:
        if force or expr not in self._alias_node_mapping:
            self._alias_node_mapping.add(node=expr, value=f"res_{next(self._alias_cnt)}")
        alias = self._alias_node_mapping.get(node=expr)
        assert alias is not None
        return alias

    def _update_alias(self, formula: _COMPILED_FORMULA_INFO_TV, force: bool = False) -> _COMPILED_FORMULA_INFO_TV:
        alias = self._get_expression_alias(expr=formula.formula_obj, force=force)
        formula = formula.clone(alias=alias)
        return formula

    def _replace_wrapped_formula(
        self,
        formula: CompiledFormulaInfo,
        expr: formula_nodes.FormulaItem,
        override_alias: Optional[str] = None,
    ) -> CompiledFormulaInfo:
        alias_kwarg = dict(alias=override_alias) if override_alias else {}
        return formula.clone(
            formula_obj=formula_nodes.Formula.make(expr=expr),
            **alias_kwarg,
        )

    def _get_override_alias(self, select_wrapper: SelectWrapperSpec) -> Optional[str]:
        if select_wrapper.type == SelectValueType.min:
            return "min_value"
        elif select_wrapper.type == SelectValueType.max:
            return "max_value"
        return None

    def _make_compiled_formula(
        self,
        field_id: FieldId,
        select_wrapper: SelectWrapperSpec = SelectWrapperSpec(type=SelectValueType.plain),
    ) -> CompiledFormulaInfo:
        formula: CompiledFormulaInfo
        # All of these types require the actual field object
        field = self._dataset.result_schema.by_guid(field_id)
        formula = self._formula_compiler.compile_field_formula(field=field)
        expr = formula.formula_obj.expr
        wrapped_expr = self._wrapper_applicator.apply_wrapper(expr, wrapper=select_wrapper)
        override_alias = self._get_override_alias(select_wrapper=select_wrapper)
        if wrapped_expr is not expr or override_alias is not None:
            formula = self._replace_wrapped_formula(formula, expr=wrapped_expr, override_alias=override_alias)

        # Override aliases with unique short ones
        formula = self._update_alias(formula)
        return formula

    def _make_compiled_order_by_formula(
        self,
        field_id: FieldId,
        direction: OrderDirection,
        select_wrapper: SelectWrapperSpec = SelectWrapperSpec(type=SelectValueType.plain),
    ) -> CompiledOrderByFormulaInfo:
        formula = self._make_compiled_formula(field_id=field_id, select_wrapper=select_wrapper)
        formula = attrs_evolve_to_subclass(
            cls=CompiledOrderByFormulaInfo,
            inst=formula,
            direction=direction,
        )
        return formula

    def _make_compiled_join_on_formula(self, relation_id: RelationId) -> CompiledJoinOnFormulaInfo:
        relation = self._ds_accessor.get_avatar_relation_strict(relation_id=relation_id)
        formula = self._formula_compiler.compile_relation_formula(relation=relation)
        return formula

    def _make_select(self, query_spec: QuerySpec) -> list[CompiledFormulaInfo]:
        result: list[CompiledFormulaInfo] = []
        used_aliases: set[str] = set()
        copy_counter = it_count()
        for field_spec in query_spec.select_specs:
            formula = self._make_compiled_formula(
                field_id=field_spec.field_id,
                select_wrapper=field_spec.wrapper,
            )
            alias = formula.not_none_alias
            if alias in used_aliases:
                alias = f"{alias}_cp{next(copy_counter)}"
                formula = formula.clone(alias=alias)

            assert alias not in used_aliases
            used_aliases.add(alias)
            result.append(formula)

        return result

    def _make_group_by(self, query_spec: QuerySpec) -> list[CompiledFormulaInfo]:
        return [
            self._make_compiled_formula(
                field_id=field_spec.field_id,
                select_wrapper=field_spec.wrapper,
            )
            for field_spec in query_spec.group_by_specs
        ]

    def _make_order_by(self, query_spec: QuerySpec) -> list[CompiledOrderByFormulaInfo]:
        result: list[CompiledOrderByFormulaInfo] = []
        already_used_ob_exprs = NodeSet()
        for order_by_spec in query_spec.order_by_specs:
            ob_expr = self._make_compiled_order_by_formula(
                field_id=order_by_spec.field_id,
                select_wrapper=order_by_spec.wrapper,
                direction=order_by_spec.direction,
            )
            if ob_expr.formula_obj in already_used_ob_exprs:
                # Deduplicate ORDER BY items
                continue
            result.append(ob_expr)
            already_used_ob_exprs.add(ob_expr.formula_obj)
        return result

    def _make_join_on(self, query_spec: QuerySpec) -> list[CompiledJoinOnFormulaInfo]:
        return [
            self._update_alias(
                self._make_compiled_join_on_formula(
                    relation_id=relation_spec.relation_id,
                )
            )
            for relation_spec in query_spec.relation_specs
        ]

    def _make_filters(self, query_spec: QuerySpec) -> list[CompiledFormulaInfo]:
        result: list[CompiledFormulaInfo] = []

        # Source filters
        for scf_spec in query_spec.source_column_filter_specs:
            formula = self._filter_compiler.compile_source_column_filter_formula(source_column_filter_spec=scf_spec)
            formula = self._update_alias(formula)
            result.append(formula)

        # Regular filters
        for filter_spec in query_spec.filter_specs:
            formula = self._filter_compiler.compile_filter_formula(filter_spec=filter_spec)
            formula = self._update_alias(formula)
            result.append(formula)

        return result

    def make_compiled_query(self, query_spec: QuerySpec) -> CompiledQuery:
        select = self._make_select(query_spec)
        group_by = self._make_group_by(query_spec)
        order_by = self._make_order_by(query_spec)
        filters = self._make_filters(query_spec)
        join_on = self._make_join_on(query_spec)

        used_avatar_ids = set(
            chain.from_iterable(
                formula.avatar_ids
                for expr_list in (select, group_by, order_by, filters, join_on)
                for formula in expr_list  # type: ignore  # TODO: fix
            )
        )

        joined_from = make_joined_from_for_avatars(
            used_avatar_ids=used_avatar_ids,
            ds_accessor=self._ds_accessor,
            root_avatar_id=query_spec.root_avatar_id,
            column_reg=self._column_reg,
        )

        compiled_query = CompiledQuery(
            id=BASE_QUERY_ID,
            level_type=ExecutionLevel.source_db,
            select=select,
            group_by=group_by,
            order_by=order_by,
            filters=filters,
            join_on=join_on,
            joined_from=joined_from,
            limit=query_spec.limit,
            offset=query_spec.offset,
            meta=query_spec.meta,
        )
        return compiled_query
