from __future__ import annotations

from typing import (
    Callable,
    Optional,
)

import attr

from dl_core.db.elements import SchemaColumn
from dl_core.query.expression import (
    ExpressionCtx,
    JoinOnExpressionCtx,
    OrderByExpressionCtx,
)
from dl_core.utils import (
    attrs_evolve_to_subclass,
    attrs_evolve_to_superclass,
)
from dl_formula.core.dialect import DialectCombo
import dl_formula.core.exc as formula_exc
from dl_formula.definitions.flags import ContextFlag
from dl_formula.inspect.env import InspectionEnvironment
from dl_formula.inspect.expression import is_aggregate_expression
from dl_formula.translation.env import (
    TranslationEnvironment,
    TranslationStats,
)
from dl_formula.translation.translator import translate
from dl_query_processing.column_registry import ColumnRegistry
from dl_query_processing.compilation.primitives import (
    CompiledFormulaInfo,
    CompiledJoinOnFormulaInfo,
    CompiledOrderByFormulaInfo,
    CompiledQuery,
)
from dl_query_processing.compilation.type_mapping import (
    DEFAULT_DATA_TYPE,
    FORMULA_TO_BI_TYPES,
)
import dl_query_processing.exc
from dl_query_processing.translation.primitives import (
    DetailedType,
    ExpressionCtxExt,
    TranslatedFlatQuery,
    TranslatedJoinedFromObject,
    TranslatedQueryMetaInfo,
)


@attr.s
class FlatQueryTranslator:
    _columns: ColumnRegistry = attr.ib(kw_only=True)
    _inspect_env: Optional[InspectionEnvironment] = attr.ib(kw_only=True, factory=InspectionEnvironment)  # noqa
    _function_scopes: int = attr.ib(kw_only=True)
    _dialect: DialectCombo = attr.ib(kw_only=True)
    # Generated attributes
    _trans_env: TranslationEnvironment = attr.ib(init=False)
    _avatar_alias_mapper: Callable[[str], str] = attr.ib(kw_only=True)
    _collect_stats: bool = attr.ib(kw_only=True, default=False)

    def __attrs_post_init__(self) -> None:
        self._trans_env = self._make_trans_env()

    def _make_trans_env(self) -> TranslationEnvironment:
        return TranslationEnvironment(
            dialect=self._dialect,
            required_scopes=self._function_scopes,
            field_types=self._columns.get_column_formula_types(),
            field_names=self._columns.get_multipart_column_names(  # type: ignore  # TODO: fix
                avatar_alias_mapper=self._avatar_alias_mapper
            ),
        )

    def translate_formula(
        self,
        comp_formula: CompiledFormulaInfo,
        collect_errors: bool = False,
        as_condition: bool = False,
    ) -> ExpressionCtxExt:
        context_flags = 0
        if as_condition:  # For MSSQL & ORACLE
            context_flags = ContextFlag.REQ_CONDITION

        try:
            translation_ctx = translate(
                formula=comp_formula.formula_obj,
                collect_errors=collect_errors,
                collect_stats=self._collect_stats,
                context_flags=context_flags,
                env=self._trans_env,
            )
        except formula_exc.TranslationError as err:
            raise dl_query_processing.exc.FormulaHandlingError(*err.errors) from err

        user_type = FORMULA_TO_BI_TYPES.get(translation_ctx.data_type, FORMULA_TO_BI_TYPES[DEFAULT_DATA_TYPE])  # type: ignore  # TODO: fix

        # Use ExpressionCtxExt because the formula might be used in SELECT
        return ExpressionCtxExt(
            expression=translation_ctx.expression,  # type: ignore  # TODO: fix
            formula_data_type=translation_ctx.data_type,
            formula_data_type_params=translation_ctx.data_type_params,
            user_type=user_type,
            alias=comp_formula.alias,
            avatar_ids=list(comp_formula.avatar_ids),
            original_field_id=comp_formula.original_field_id,
        )

    def translate_order_by_formula(
        self,
        comp_formula: CompiledOrderByFormulaInfo,
        collect_errors: bool = False,
    ) -> OrderByExpressionCtx:
        assert isinstance(comp_formula, CompiledOrderByFormulaInfo)
        translated_formula = self.translate_formula(comp_formula=comp_formula, collect_errors=collect_errors)
        return attrs_evolve_to_subclass(
            cls=OrderByExpressionCtx,
            inst=attrs_evolve_to_superclass(cls=ExpressionCtx, inst=translated_formula),
            direction=comp_formula.direction,
        )

    def translate_join_on_formula(
        self,
        comp_formula: CompiledJoinOnFormulaInfo,
        collect_errors: bool = False,
    ) -> JoinOnExpressionCtx:
        assert isinstance(comp_formula, CompiledJoinOnFormulaInfo)
        translated_formula = self.translate_formula(
            comp_formula=comp_formula,
            collect_errors=collect_errors,
            as_condition=True,
        )
        return attrs_evolve_to_subclass(
            cls=JoinOnExpressionCtx,
            inst=attrs_evolve_to_superclass(cls=ExpressionCtx, inst=translated_formula),
            left_id=comp_formula.left_id,
            right_id=comp_formula.right_id,
            join_type=comp_formula.join_type,
        )

    def _get_detailed_types(
        self, compiled_flat_query: CompiledQuery, translated_select: list[ExpressionCtxExt]
    ) -> Optional[list[Optional[DetailedType]]]:
        field_order = compiled_flat_query.meta.field_order
        if field_order is None:
            return None

        result: list[Optional[DetailedType]] = []
        for field_idx, field_id in field_order:
            detailed_type: Optional[DetailedType]
            if field_id is None:
                detailed_type = None
            else:
                field_expr = translated_select[field_idx]
                assert field_expr.user_type is not None
                detailed_type = DetailedType(
                    field_id=field_id,
                    data_type=field_expr.user_type,
                    formula_data_type=field_expr.formula_data_type,
                    formula_data_type_params=field_expr.formula_data_type_params,
                )

            result.append(detailed_type)

        return result

    def _generate_colummn_list_for_query(
        self,
        query_id: str,
        translated_select: list[ExpressionCtxExt],
    ) -> list[SchemaColumn]:
        """
        Generate list of columns that can be used when selecting from this query
        as if it were a table.
        """

        column_list: list[SchemaColumn] = []
        for translated_formula in translated_select:
            column = SchemaColumn(
                name=translated_formula.alias,  # type: ignore  # TODO: fix
                title=translated_formula.alias,
                source_id=query_id,
                native_type=None,
                user_type=translated_formula.user_type,
                has_auto_aggregation=False,
                lock_aggregation=False,
            )
            column_list.append(column)

        return column_list

    def translate_flat_query(
        self,
        compiled_flat_query: CompiledQuery,
        collect_errors: bool = False,
    ) -> TranslatedFlatQuery:
        where: list[CompiledFormulaInfo] = []
        having: list[CompiledFormulaInfo] = []
        for formula in compiled_flat_query.filters:
            if is_aggregate_expression(node=formula.formula_obj, env=self._inspect_env):  # type: ignore  # TODO: fix
                having.append(formula)
            else:
                where.append(formula)

        translated_select = [
            self.translate_formula(formula, collect_errors=collect_errors) for formula in compiled_flat_query.select
        ]

        column_list = self._generate_colummn_list_for_query(
            query_id=compiled_flat_query.id,
            translated_select=translated_select,
        )

        translated_meta = TranslatedQueryMetaInfo.from_comp_meta(
            compiled_flat_query.meta,
            detailed_types=self._get_detailed_types(
                compiled_flat_query=compiled_flat_query,
                translated_select=translated_select,
            ),
        )

        alias = self._avatar_alias_mapper(compiled_flat_query.id)
        return TranslatedFlatQuery(
            id=compiled_flat_query.id,
            alias=alias,
            level_type=compiled_flat_query.level_type,
            select=translated_select,
            group_by=[
                self.translate_formula(formula, collect_errors=collect_errors)
                for formula in compiled_flat_query.group_by
            ],
            order_by=[
                self.translate_order_by_formula(formula, collect_errors=collect_errors)
                for formula in compiled_flat_query.order_by
            ],
            where=[
                self.translate_formula(formula, collect_errors=collect_errors, as_condition=True) for formula in where
            ],
            having=[
                self.translate_formula(formula, collect_errors=collect_errors, as_condition=True) for formula in having
            ],
            join_on=[
                self.translate_join_on_formula(formula, collect_errors=collect_errors)
                for formula in compiled_flat_query.join_on
            ],
            joined_from=TranslatedJoinedFromObject(
                root_from_id=compiled_flat_query.joined_from.root_from_id,
                froms=compiled_flat_query.joined_from.froms,
            ),
            limit=compiled_flat_query.limit,
            offset=compiled_flat_query.offset,
            column_list=column_list,
            meta=translated_meta,
        )

    def get_collected_stats(self) -> TranslationStats:
        return self._trans_env.translation_stats
