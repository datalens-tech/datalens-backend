import attr
import sqlalchemy as sa
from sqlalchemy.sql.elements import ClauseElement

from bi_formula.core.dialect import DialectCombo
from bi_formula.definitions.scope import Scope
from bi_formula.translation.env import TranslationEnvironment
from bi_formula.translation.translator import SqlAlchemyTranslator

from bi_formula_ref.examples.query import (
    RawQueryContext, CompiledQueryContext, FormulaContext, TableReference,
)


@attr.s
class SaQueryCompiler:
    dialect: DialectCombo = attr.ib(kw_only=True)
    _function_scopes: int = attr.ib(kw_only=True, default=Scope.EXPLICIT_USAGE)

    def _get_translator(self, table_ref: TableReference) -> SqlAlchemyTranslator:
        translator = SqlAlchemyTranslator(
            env=TranslationEnvironment(
                dialect=self.dialect,
                required_scopes=self._function_scopes,
                field_names={col.name: (table_ref.name, col.name) for col in table_ref.columns},
                field_types={col.name: col.data_type for col in table_ref.columns},
                field_type_params={},
            ),
        )
        return translator

    def compile_formula(
            self, formula_ctx: FormulaContext, translator: SqlAlchemyTranslator
    ) -> ClauseElement:
        ctx = translator.translate(formula=formula_ctx.formula)
        expr = ctx.expression
        assert expr is not None
        return expr

    def compile_from(self, table_name: str) -> ClauseElement:
        return sa.table(name=table_name)

    def compile_query(self, raw_query: RawQueryContext, table_ref: TableReference) -> CompiledQueryContext:
        trans = self._get_translator(table_ref=table_ref)
        sa_select = [self.compile_formula(formula_ctx, translator=trans) for formula_ctx in raw_query.select]
        sa_group_by = [self.compile_formula(formula_ctx, translator=trans) for formula_ctx in raw_query.group_by]
        sa_order_by = [self.compile_formula(formula_ctx, translator=trans) for formula_ctx in raw_query.order_by]
        sa_from = self.compile_from(table_name=table_ref.name)
        sa_query = sa.select(sa_select).select_from(sa_from).group_by(*sa_group_by).order_by(*sa_order_by)
        compiled_query_ctx = CompiledQueryContext(
            result_columns=raw_query.result_columns,
            sa_query=sa_query,
        )
        return compiled_query_ctx
