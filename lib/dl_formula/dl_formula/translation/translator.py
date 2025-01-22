from __future__ import annotations

import datetime
from functools import singledispatchmethod
from itertools import chain
from typing import (
    Optional,
    Union,
)

import sqlalchemy as sa
from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy.sql.functions import Function

import dl_formula.core.aux_nodes as aux_nodes
from dl_formula.core.datatype import (
    DataType,
    DataTypeParams,
)
from dl_formula.core.dialect import DialectCombo
import dl_formula.core.exc as exc
import dl_formula.core.nodes as nodes
from dl_formula.definitions.common import desc
from dl_formula.definitions.literals import literal
from dl_formula.definitions.registry import OPERATION_REGISTRY
from dl_formula.definitions.scope import Scope
from dl_formula.definitions.type_strategy import FromArgs
import dl_formula.inspect.literal
from dl_formula.translation import ext_nodes
from dl_formula.translation.columns import get_column_renderer
from dl_formula.translation.context import TranslationCtx
from dl_formula.translation.context_processing import get_context_processor
from dl_formula.translation.env import (
    FunctionStatsSignature,
    TranslationEnvironment,
)
from dl_formula.translation.sa_dialects import get_sa_dialect


class SqlAlchemyTranslator:
    default_restrict_funcs = True
    default_restrict_fields = True

    def __init__(
        self,
        env: TranslationEnvironment,
        restrict_funcs: Optional[bool] = None,
        restrict_fields: Optional[bool] = None,
        collect_stats: bool = False,
    ):
        self._env = env
        self._restrict_funcs = restrict_funcs if restrict_funcs is not None else self.default_restrict_funcs
        self._restrict_fields = restrict_fields if restrict_fields is not None else self.default_restrict_fields
        self._collect_stats = collect_stats

        self._postprocessor = get_context_processor(dialect=self._env.dialect)
        self._column_renderer = get_column_renderer(dialect=self._env.dialect, field_names=self._env.field_names)

    def _generate_default_function(self, node: nodes.OperationCall, translated_args: list[TranslationCtx]) -> Function:
        return Function(node.name, *translated_args)

    def simple_translate_func(self, node: nodes.OperationCall, ctx: TranslationCtx) -> None:
        """
        Translate function call using ``OPERATION_REGISTRY``:
        - check its name against a list of allowed functions
        - check argument types (optional)
        - check argument count
        """

        def translator_cb(obj: nodes.FormulaItem) -> Union[nodes.FormulaItem, ClauseElement]:
            """Callback that can translate a formula node for usage from function implementation"""
            return self.translate_node(  # type: ignore  # TODO: fix
                obj,
                ctx=TranslationCtx(node=obj, required_scopes=ctx.required_scopes & ~Scope.EXPLICIT_USAGE),
                postprocess=False,
            ).expression

        func_name = node.name
        ctx.set_token(func_name)
        ctx.fork()

        translated_dimensions = []
        translated_order_by = []
        if isinstance(node, nodes.WindowFuncCall):
            grouping = node.grouping
            if isinstance(grouping, nodes.WindowGroupingTotal):
                pass  # empty dimension list
            elif isinstance(grouping, nodes.WindowGroupingWithin):
                translated_dimensions = [
                    self.translate_node(dim, ctx=ctx.child(node=dim), postprocess=False).expression
                    for dim in grouping.dim_list
                ]
            else:
                raise TypeError(f"{type(grouping)} is not supported as a grouping type")

            translated_order_by = [
                self.translate_node(dim, ctx=ctx.child(node=dim), postprocess=False).expression
                for dim in node.ordering.children
            ]

        translated_args = [self.translate_node(arg, ctx=ctx.child(node=arg), postprocess=False) for arg in node.args]
        arg_types = [arg.data_type for arg in translated_args]

        if self._collect_stats:
            dialect_name = self._env.dialect.single_bit.name.name if self._env.dialect.deterministic else ""
            signature = FunctionStatsSignature(
                name=func_name,
                arg_types=tuple(at.name for at in arg_types),  # type: ignore  # TODO: fix
                dialect=dialect_name,
                is_window=isinstance(node, nodes.WindowFuncCall),
            )
            self._env.translation_stats.add_function_hit(signature)

        # find matching function in the registry
        try:
            func_definition = OPERATION_REGISTRY.get_definition(
                name=func_name,
                arg_types=arg_types,  # type: ignore  # TODO: fix
                is_window=isinstance(node, nodes.WindowFuncCall),
                dialect=self._env.dialect,
                required_scopes=ctx.required_scopes,
            )
            translation_result = func_definition.translate(env=self._env, args=translated_args)
            translated_args = translation_result.transformed_args
            ctx.set_type(translation_result.data_type, translation_result.data_type_params)
            ctx.set_flags(translation_result.context_flags)
            # postprocess nodes after the translation may have updated argument context flags
            if translation_result.postprocess_args:
                # Postprocessing may be switched of if this function redirects to another
                for child_ctx in translated_args:
                    self._postprocess_context(child_ctx)

            translated_func = translation_result.impl_callable(
                *translated_args,
                translator_cb=translator_cb,
                partition_by=translated_dimensions,
                default_order_by=translated_order_by,
                translation_ctx=ctx,
                translation_env=self._env,
            )
            # Here `translated_func` can be a lot of different things (SQLA objects)
            # Even for window functions we cannot rely on it being an `Over` instance

        except exc.TranslationError as err:
            # One of:
            # - no such function,
            # - wrong argument types
            # - error during translation (possibly errors in nested elements)
            if isinstance(err, exc.TranslationUnknownFunctionError) and not self._restrict_funcs:
                translated_func = self._generate_default_function(node, translated_args=translated_args)
            else:
                for each_error in err.errors:
                    ctx.add_error(each_error.message, token=each_error.token or func_name, code=each_error.code)
                translated_func = sa.null()
            ctx.set_type(DataType.NULL)

        ctx.set_expression(translated_func)

    def translate(
        self,
        formula: nodes.Formula,
        collect_errors: Optional[bool] = None,
        context_flags: Optional[int] = None,
    ) -> TranslationCtx:
        """
        Translate ``Formula`` object into an SQLAlchemy selectable,
        which then can be compiled into valid RAW SQL.
        """
        context_flags = context_flags or 0
        ctx = TranslationCtx(
            required_scopes=self._env.required_scopes,
            collect_errors=collect_errors,
            flags=context_flags,
            node=formula,
        )
        self.translate_node(formula.expr, ctx)
        if ctx.errors:
            raise exc.TranslationError(*ctx.errors)

        if isinstance(ctx.expression, (int, float, bool, str, datetime.datetime, datetime.date, type(None))):
            ctx.set_expression(literal(ctx.expression))

        self._coerce_type(ctx)
        return ctx

    @staticmethod
    def _coerce_type(ctx: TranslationCtx) -> None:
        """
        Make sure value is cast to proper Python type
        (this is done outside the DB by ``sqlalchemy``)
        """
        if ctx.data_type in (DataType.CONST_INTEGER, DataType.INTEGER):
            ctx.set_expression(sa.type_coerce(ctx.expression, sa.Integer))
        elif ctx.data_type in (DataType.CONST_FLOAT, DataType.FLOAT):
            ctx.set_expression(sa.type_coerce(ctx.expression, sa.Float))
        elif ctx.data_type in (DataType.CONST_BOOLEAN, DataType.BOOLEAN):
            pass  # coercion to BOOL results in extra ' = 1', so don't do it
        elif ctx.data_type in (DataType.CONST_DATE, DataType.DATE):
            ctx.set_expression(sa.type_coerce(ctx.expression, sa.Date))
        elif ctx.data_type in (DataType.CONST_DATETIME, DataType.DATETIME):
            ctx.set_expression(sa.type_coerce(ctx.expression, sa.DateTime))
        elif ctx.data_type in (DataType.CONST_GENERICDATETIME, DataType.GENERICDATETIME):
            ctx.set_expression(sa.type_coerce(ctx.expression, sa.DateTime))

    def translate_node(self, node: nodes.FormulaItem, ctx: TranslationCtx, postprocess: bool = True) -> TranslationCtx:
        """Wrapper for internal implementation of translation methods"""

        # check replacements and cache first
        found_translation_ctx = self._env.replacements.get(node)
        if found_translation_ctx is None:
            found_translation_ctx = self._env.translation_cache.get(node)
            if self._collect_stats:
                self._env.translation_stats.add_cache_hit()
        if found_translation_ctx is not None:
            # found in cache, so reuse it
            ctx.adopt(found_translation_ctx)
        else:
            # not found, so translate it
            self._translate_node(node, ctx)

        ctx.flush()
        try:
            self._env.translation_cache.add(node=node, value=ctx.copy())
        except exc.CacheError:
            pass  # node is not cacheable

        if postprocess:
            self._postprocess_context(ctx)

        return ctx

    def _postprocess_context(self, ctx: TranslationCtx) -> None:
        """Additional contextual postprocessing of expressions (dialect dependent)"""

        assert ctx.data_type is not None
        assert ctx.expression is not None

        expression, flags = self._postprocessor.postprocess_expression(
            data_type=ctx.data_type, expression=ctx.expression, flags=ctx.flags
        )
        if expression is not ctx.expression or flags != ctx.flags:
            ctx.set_expression(expression)
            ctx.set_flags(flags)

        for warning in self._postprocessor.get_warnings(data_type=ctx.data_type, flags=ctx.flags):
            ctx.add_warning(warning)

    @singledispatchmethod
    def _translate_node(self, node: nodes.FormulaItem, ctx: TranslationCtx) -> None:
        """
        Default action for singledispatchmethod.
        Is called if ``node`` doesn't match any of the registered types.
        In this case we assume that it has already been translated, so return it as-is
        """
        raise TypeError(type(node))

    @_translate_node.register(TranslationCtx)
    def _translate_translation_ctx(self, node: TranslationCtx, ctx: TranslationCtx) -> None:
        """Already translated node. Do nothing"""
        pass

    @_translate_node.register(nodes.LiteralInteger)
    @_translate_node.register(nodes.LiteralFloat)
    @_translate_node.register(nodes.LiteralBoolean)
    @_translate_node.register(nodes.LiteralString)
    @_translate_node.register(nodes.LiteralDate)
    @_translate_node.register(nodes.LiteralDatetime)
    @_translate_node.register(nodes.LiteralDatetimeTZ)
    @_translate_node.register(nodes.LiteralGenericDatetime)
    @_translate_node.register(nodes.LiteralGeopoint)
    @_translate_node.register(nodes.LiteralGeopolygon)
    @_translate_node.register(nodes.LiteralUuid)
    @_translate_node.register(nodes.LiteralArrayInteger)
    @_translate_node.register(nodes.LiteralArrayFloat)
    @_translate_node.register(nodes.LiteralArrayString)
    @_translate_node.register(nodes.LiteralTreeString)
    def _translate_node_literal(self, node: nodes.BaseLiteral, ctx: TranslationCtx) -> None:
        data_type_params: Optional[DataTypeParams] = None
        if isinstance(node, nodes.LiteralDatetimeTZ):
            data_type_params = DataTypeParams(timezone="UTC")  # not certain
        ctx.set_type(
            dl_formula.inspect.literal.get_data_type(node),
            data_type_params=data_type_params,
        )
        value = node.value
        if isinstance(node, nodes.LiteralUuid):
            value = str(value)
        ctx.set_expression(literal(value, d=self._env.dialect))

    @_translate_node.register(ext_nodes.CompiledExpression)
    def _translate_node_compiled_expression(self, node: ext_nodes.CompiledExpression, ctx: TranslationCtx) -> None:
        if isinstance(node.value, TranslationCtx):
            ctx.adopt(node.value)
        else:
            ctx.set_expression(node.value)

    @_translate_node.register(nodes.Null)
    def _translate_node_null(self, node: Optional[nodes.Null], ctx: TranslationCtx) -> None:
        ctx.set_type(DataType.NULL)
        ctx.set_expression(sa.null())

    @_translate_node.register(nodes.ExpressionList)
    def _translate_node_expression_list(self, node: nodes.ExpressionList, ctx: TranslationCtx) -> None:
        ctx.fork()
        translated_child_list = []
        data_types = []
        for item in node.children:
            translated_child = self.translate_node(item, ctx=ctx.child(node=item))
            translated_child_list.append(translated_child.expression)
            data_types.append(translated_child.data_type)

        ctx.set_expression(translated_child_list)
        ctx.set_type(FromArgs().get_from_args(data_types))  # type: ignore  # TODO: fix  # TODO?: data_type_params

    def _make_literal_column(self, name: str) -> sa.sql.ClauseElement:
        return self._column_renderer.make_column(name)

    @_translate_node.register(nodes.Field)
    def _translate_node_field(self, node: nodes.Field, ctx: TranslationCtx) -> None:
        field_id = node.name
        ctx.set_token(field_id)
        try:
            ctx.set_expression(self._make_literal_column(field_id))
            ctx.set_type(self._env.field_types[field_id], self._env.field_type_params.get(field_id))
        except KeyError:
            if self._restrict_fields:
                name_parts = self._env.field_names.get(field_id) or (field_id,)
                ctx.add_error(
                    f'Translator: no info about column: {".".join(name_parts)}',
                    code=exc.TranslationUnknownFieldError.default_code,
                )
            ctx.set_expression(sa.literal(field_id))
            ctx.set_type(DataType.NULL)

    @_translate_node.register(nodes.Unary)
    @_translate_node.register(nodes.Binary)
    @_translate_node.register(nodes.Ternary)
    @_translate_node.register(nodes.FuncCall)
    @_translate_node.register(nodes.WindowFuncCall)
    def _translate_node_func_call(self, node: nodes.FuncCall, ctx: TranslationCtx) -> None:
        self.simple_translate_func(node=node, ctx=ctx)

    @_translate_node.register(nodes.CaseBlock)
    def _translate_node_case_block(self, node: nodes.CaseBlock, ctx: TranslationCtx) -> None:
        args: list[nodes.FormulaItem] = [
            node.case_expr,
            *chain.from_iterable(when_part.children for when_part in node.when_list),
        ]
        if node.else_expr is not None:
            args.append(node.else_expr)
        self.simple_translate_func(
            node=nodes.FuncCall.make("_case_block_", args=args), ctx=ctx.child(required_scopes=ctx.required_scopes)
        )

    @_translate_node.register(nodes.IfBlock)
    def _translate_node_if_block(self, node: nodes.IfBlock, ctx: TranslationCtx) -> None:
        args: list[nodes.FormulaItem] = [
            *chain.from_iterable(if_part.children for if_part in node.if_list),
            node.else_expr,
        ]
        self.simple_translate_func(
            node=nodes.FuncCall.make("_if_block_", args=args),
            ctx=ctx.child(required_scopes=ctx.required_scopes),
        )

    @_translate_node.register(nodes.ParenthesizedExpr)
    def _translate_node_parenthesized_expr(self, node: nodes.ParenthesizedExpr, ctx: TranslationCtx) -> None:
        self.translate_node(node.expr, ctx=ctx, postprocess=False)

    @_translate_node.register(nodes.OrderAscending)
    @_translate_node.register(nodes.OrderDescending)
    def _translate_node_ordering_direction(self, node: nodes.OrderingDirectionBase, ctx: TranslationCtx) -> None:
        ctx.fork()
        child = self.translate_node(node.expr, ctx=ctx.child(node=node.expr), postprocess=True)
        expression = child.expression
        if isinstance(node, nodes.OrderDescending):
            expression = desc(expression)  # type: ignore  # TODO: fix
        ctx.set_expression(expression)
        ctx.set_type(child.data_type, child.data_type_params)  # type: ignore  # TODO: fix

    @_translate_node.register(aux_nodes.ErrorNode)
    def _translate_node_error(self, node: aux_nodes.ErrorNode, ctx: TranslationCtx) -> None:
        ctx.set_expression(sa.null())
        ctx.set_type(DataType.NULL)
        ctx.add_error(message=node.message, code=node.err_code)


def translate(
    formula: nodes.Formula,
    dialect: Optional[DialectCombo] = None,
    required_scopes: int = Scope.EXPLICIT_USAGE,
    restrict_funcs: Optional[bool] = None,
    restrict_fields: Optional[bool] = None,
    collect_errors: Optional[bool] = None,
    collect_stats: bool = False,
    field_types: Optional[dict[str, DataType]] = None,
    context_flags: Optional[int] = None,
    field_names: Optional[dict[str, tuple[str, ...]]] = None,
    env: Optional[TranslationEnvironment] = None,
) -> TranslationCtx:
    """Translate ``Formula`` tree object into an SQLAlchemy representation that can be used for queries"""

    if env is None:
        assert dialect is not None
        env = TranslationEnvironment(
            dialect=dialect,
            required_scopes=required_scopes,
            field_types=field_types or {},
            field_names=field_names or {},
        )
    assert env is not None

    translator = SqlAlchemyTranslator(
        env=env,
        restrict_funcs=restrict_funcs,
        restrict_fields=restrict_fields,
        collect_stats=collect_stats,
    )
    return translator.translate(
        formula,
        collect_errors=collect_errors,
        context_flags=context_flags,
    )


def translate_and_compile(
    formula: nodes.Formula,
    dialect: Optional[DialectCombo] = None,
    required_scopes: int = Scope.EXPLICIT_USAGE,
    restrict_funcs: Optional[bool] = None,
    restrict_fields: Optional[bool] = None,
    collect_errors: Optional[bool] = None,
    collect_stats: bool = False,
    field_types: Optional[dict[str, DataType]] = None,
    context_flags: Optional[int] = None,
    field_names: Optional[dict[str, tuple[str, ...]]] = None,
    env: Optional[TranslationEnvironment] = None,
) -> str:
    """Translate ``Formula`` tree object into an SQLAlchemy representation and compile it into raw SQL"""

    if env is None:
        assert dialect is not None
        env = TranslationEnvironment(
            dialect=dialect,
            required_scopes=required_scopes,
            field_types=field_types or {},
            field_names=field_names or {},
        )
    assert env is not None

    translator = SqlAlchemyTranslator(
        env=env,
        restrict_funcs=restrict_funcs,
        restrict_fields=restrict_fields,
        collect_stats=collect_stats,
    )
    result = translator.translate(
        formula,
        collect_errors=collect_errors,
        context_flags=context_flags,
    )
    sa_dialect = get_sa_dialect(env.dialect)
    sa_compiled_expr = result.expression.compile(  # type: ignore  # TODO: fix
        compile_kwargs={"literal_binds": True},
        dialect=sa_dialect,
    )
    compiled_text = sa_compiled_expr.string
    return compiled_text
