from __future__ import annotations

import datetime
from typing import (
    Iterable,
    Optional,
)

import antlr4
from antlr4.Token import Token
from antlr4.tree.Tree import TerminalNodeImpl

import dl_formula.core.exc as exc
import dl_formula.core.nodes as nodes
from dl_formula.core.position import (
    Position,
    PositionConverter,
)
from dl_formula.parser.base import resolve_function_capabilities
from dl_formula.utils.datetime import make_datetime_value


try:
    from dl_formula.parser.antlr.gen.DataLensParser import DataLensParser
    from dl_formula.parser.antlr.gen.DataLensVisitor import DataLensVisitor
except ImportError as e:
    raise exc.ParserNotFoundError() from e


EMPTY_FORMULA_ERROR = "Empty formula is invalid"

COMPARISON_OP_RAW_NAMES = ("=", "!=", "<>", ">", ">=", "<", "<=")


class CustomDataLensVisitor(DataLensVisitor):
    _BOOL = {"true": True, "false": False}

    def __init__(self, text: str):
        self._text = text
        self._pos_conv = PositionConverter(text=text)

    def _make_node_meta(self, *ctxes: antlr4.ParserRuleContext) -> Optional[nodes.NodeMeta]:
        first_ctx = ctxes[0]
        last_ctx = ctxes[-1]
        return nodes.NodeMeta(
            position=self._make_position(*ctxes),
            original_text=self._text[first_ctx.start.start : last_ctx.stop.stop + 1],
        )

    def _make_position(self, *ctxes: antlr4.ParserRuleContext) -> Optional[Position]:
        first_ctx = ctxes[0]
        last_ctx = ctxes[-1]
        return self._pos_conv.merge_positions(
            start_position=self._pos_conv.idx_to_position(first_ctx.start.start),
            end_position=self._pos_conv.idx_to_position(last_ctx.stop.stop + 1),
        )

    def _separate_children(
        self,
        ctx: antlr4.ParserRuleContext,
        exclude: Iterable[str] = (),
        lower_str: bool = False,
    ) -> tuple[list[str], list[nodes.FormulaItem]]:
        str_children = []
        node_children = []
        for child in ctx.children:
            if isinstance(child, TerminalNodeImpl):
                child_str = str(child)
                if lower_str:
                    child_str = child_str.lower()
                if child_str not in exclude:
                    str_children.append(child_str)
            else:
                node_children.append(self.visit(child))

        return str_children, node_children

    def visitIntegerLiteral(self, ctx: DataLensParser.IntegerLiteralContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        return nodes.LiteralInteger.make(value=int(str(ctx.children[0])), meta=self._make_node_meta(ctx))

    def visitFloatLiteral(self, ctx: DataLensParser.FloatLiteralContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        return nodes.LiteralFloat.make(value=float(str(ctx.children[0])), meta=self._make_node_meta(ctx))

    def visitStringLiteral(self, ctx: DataLensParser.StringLiteralContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        s = str(ctx.children[0])[1:-1]  # strip off the quotes
        s = s.replace("\\'", "'").replace('\\"', '"')
        s = s.replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t")
        s = s.replace("\\\\", "\\")
        return nodes.LiteralString.make(value=s, meta=self._make_node_meta(ctx))

    def visitDateLiteral(self, ctx: DataLensParser.DateLiteralContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        date_str = str(ctx.children[1])
        try:
            value = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError as e:
            raise exc.ParseDateValueError(
                f"Invalid date value: {date_str}", token=date_str, position=self._make_position(ctx)
            ) from e
        return nodes.LiteralDate.make(value=value, meta=self._make_node_meta(ctx))

    def visitDatetimeLiteral(self, ctx: DataLensParser.DatetimeLiteralContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        date_str = str(ctx.children[1])
        try:
            value = make_datetime_value(date_str)
            assert value is not None
        except ValueError as e:
            raise exc.ParseDatetimeValueError(
                f"Invalid datetime value: {date_str}", token=date_str, position=self._make_position(ctx)
            ) from e
        node_cls = nodes.LiteralGenericDatetime
        return node_cls.make(value=value, meta=self._make_node_meta(ctx))

    def visitGenericDateLiteral(self, ctx: DataLensParser.GenericDateLiteralContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        date_str = str(ctx.children[1])
        try:
            value = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError as e:
            raise exc.ParseDateValueError(
                f"Invalid date value: {date_str}", token=date_str, position=self._make_position(ctx)
            ) from e
        return nodes.LiteralDate.make(value=value, meta=self._make_node_meta(ctx))

    def visitGenericDatetimeLiteral(self, ctx: DataLensParser.GenericDatetimeLiteralContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        date_str = str(ctx.children[1])
        try:
            value = make_datetime_value(date_str)
            assert value is not None
        except ValueError as e:
            raise exc.ParseDatetimeValueError(
                f"Invalid datetime value: {date_str}", token=date_str, position=self._make_position(ctx)
            ) from e
        node_cls = nodes.LiteralGenericDatetime
        return node_cls.make(value=value, meta=self._make_node_meta(ctx))

    def visitBoolLiteral(self, ctx: DataLensParser.BoolLiteralContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        return nodes.LiteralBoolean.make(value=self._BOOL[str(ctx.children[0]).lower()], meta=self._make_node_meta(ctx))

    def visitNullLiteral(self, ctx: DataLensParser.NullLiteralContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        return nodes.Null(meta=self._make_node_meta(ctx))

    def visitFieldName(self, ctx: DataLensParser.FieldNameContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        return nodes.Field.make(name=str(ctx.children[0])[1:-1], meta=self._make_node_meta(ctx))

    def visitOrderingItem(self, ctx: DataLensParser.OrderingItemContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        str_children, node_children = self._separate_children(ctx)
        expr = node_children[0]

        if str_children:
            direction = str_children[0].lower()
            if direction == "asc":
                expr = nodes.OrderAscending.make(expr=expr, meta=self._make_node_meta(ctx))
            elif direction == "desc":
                expr = nodes.OrderDescending.make(expr=expr, meta=self._make_node_meta(ctx))
            else:
                raise ValueError("Unexpected value for ordering item")

        return expr

    def visitOrdering(self, ctx: DataLensParser.OrderingContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        _, node_children = self._separate_children(ctx)
        return nodes.Ordering.make(expr_list=node_children, meta=self._make_node_meta(ctx))

    def visitLodSpecifier(self, ctx: DataLensParser.LodSpecifierContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        str_nodes, node_children = self._separate_children(ctx)
        kind = str_nodes[0].lower()

        lod_cls: type[nodes.LodSpecifier] = {
            "fixed": nodes.FixedLodSpecifier,
            "include": nodes.IncludeLodSpecifier,
            "exclude": nodes.ExcludeLodSpecifier,
        }[kind]

        return lod_cls.make(dim_list=node_children, meta=self._make_node_meta(ctx))

    def visitWinGrouping(self, ctx: DataLensParser.WinGroupingContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        str_nodes, node_children = self._separate_children(ctx)
        kind = str_nodes[0].lower()

        if kind == "total":
            return nodes.WindowGroupingTotal(meta=self._make_node_meta(ctx))

        grouping_cls = {
            "among": nodes.WindowGroupingAmong,
            "within": nodes.WindowGroupingWithin,
        }[kind]

        return grouping_cls.make(dim_list=node_children, meta=self._make_node_meta(ctx))

    def visitBeforeFilterBy(self, ctx: DataLensParser.BeforeFilterByContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        _, node_children = self._separate_children(ctx)
        field_names: list[str] = []
        for field in node_children:
            assert isinstance(field, nodes.Field)
            field_names.append(field.name)
        return nodes.BeforeFilterBy.make(field_names=frozenset(field_names), meta=self._make_node_meta(ctx))

    def visitIgnoreDimensions(self, ctx: DataLensParser.IgnoreDimensionsContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        _, node_children = self._separate_children(ctx)
        return nodes.IgnoreDimensions.make(dim_list=node_children, meta=self._make_node_meta(ctx))

    def visitFunction(self, ctx: DataLensParser.FunctionContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        str_nodes, node_children = self._separate_children(ctx)
        name = str_nodes[0].lower()

        grouping = next((arg for arg in node_children if isinstance(arg, nodes.WindowGrouping)), None)
        ordering = next((arg for arg in node_children if isinstance(arg, nodes.Ordering)), None)
        ignore_dimensions = next((arg for arg in node_children if isinstance(arg, nodes.IgnoreDimensions)), None)
        before_filter_by = next((arg for arg in node_children if isinstance(arg, nodes.BeforeFilterBy)), None)
        lod = next((arg for arg in node_children if isinstance(arg, nodes.LodSpecifier)), None)

        args = [
            arg
            for arg in node_children
            if not isinstance(
                arg,
                (
                    str,
                    nodes.WindowGrouping,
                    nodes.Ordering,
                    nodes.BeforeFilterBy,
                    nodes.IgnoreDimensions,
                    nodes.LodSpecifier,
                ),
            )
        ]

        function_capabilities = resolve_function_capabilities(
            name=name,
            grouping=grouping,
            lod=lod,
            ordering=ordering,
            before_filter_by=before_filter_by,
            ignore_dimensions=ignore_dimensions,
        )

        if function_capabilities.is_window:
            return nodes.WindowFuncCall.make(
                name=name,
                args=args,
                grouping=grouping,
                ordering=ordering,
                ignore_dimensions=ignore_dimensions,
                before_filter_by=before_filter_by,
                meta=self._make_node_meta(ctx),
            )

        return nodes.FuncCall.make(
            name=name,
            args=args,
            lod=lod,
            ignore_dimensions=ignore_dimensions,
            before_filter_by=before_filter_by,
            meta=self._make_node_meta(ctx),
        )

    def visitElsePart(self, ctx: DataLensParser.ElsePartContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        return self.visit(ctx.children[1])

    def visitIfPart(self, ctx: DataLensParser.IfPartContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        return nodes.IfPart.make(
            cond=self.visit(ctx.children[1]),
            expr=self.visit(ctx.children[3]),
            meta=self._make_node_meta(ctx),
        )

    def visitElseifPart(self, ctx: DataLensParser.ElseifPartContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        return nodes.IfPart.make(
            cond=self.visit(ctx.children[1]),
            expr=self.visit(ctx.children[3]),
            meta=self._make_node_meta(ctx),
        )

    def visitIfBlock(self, ctx: DataLensParser.IfBlockContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        # args: [*if_parts, ?else_part, "END"]
        assert len(ctx.children) >= 2

        has_explicit_else = not isinstance(
            ctx.children[-2], (DataLensParser.IfPartContext, DataLensParser.ElseifPartContext)
        )

        if has_explicit_else:
            else_expr = self.visit(ctx.children[-2])
            if_list = [self.visit(ch) for ch in ctx.children[0:-2]]
        else:
            else_expr = nodes.Null()
            if_list = [self.visit(ch) for ch in ctx.children[0:-1]]

        assert all(isinstance(item, nodes.IfPart) for item in if_list)
        return nodes.IfBlock.make(
            if_list=if_list,
            else_expr=else_expr,
            meta=self._make_node_meta(ctx),
        )

    def visitWhenPart(self, ctx: DataLensParser.WhenPartContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        # args: ["WHEN", val, "THEN", expr]
        assert len(ctx.children) == 4
        return nodes.WhenPart.make(
            val=self.visit(ctx.children[1]),
            expr=self.visit(ctx.children[3]) if len(ctx.children) > 2 else None,
            meta=self._make_node_meta(ctx),
        )

    def visitCaseBlock(self, ctx: DataLensParser.CaseBlockContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        # args: ["CASE", case_expr, *when_parts, ?else_expr, "END"]
        assert len(ctx.children) >= 4

        case_expr = self.visit(ctx.children[1])

        has_explicit_else = not isinstance(ctx.children[-2], DataLensParser.WhenPartContext)

        if has_explicit_else:
            else_expr = self.visit(ctx.children[-2])
            when_list = [self.visit(ch) for ch in ctx.children[2:-2]]
        else:
            else_expr = nodes.Null()
            when_list = [self.visit(ch) for ch in ctx.children[2:-1]]

        assert all(isinstance(item, nodes.WhenPart) for item in when_list)

        return nodes.CaseBlock.make(
            case_expr=case_expr,
            when_list=when_list,
            else_expr=else_expr,
            meta=self._make_node_meta(ctx),
        )

    def visitParenthesizedExpr(self, ctx: DataLensParser.ParenthesizedExprContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        return nodes.ParenthesizedExpr.make(
            expr=self.visit(ctx.children[1]),
            meta=self._make_node_meta(ctx),
        )

    def visitUnaryPrefix(self, ctx: DataLensParser.UnaryPrefixContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        str_children, node_children = self._separate_children(ctx, lower_str=True)
        op_name = "".join(str_children)
        expr = node_children[0]

        if op_name == "-" and isinstance(expr, (nodes.LiteralInteger, nodes.LiteralFloat)):
            # negative number
            return expr.__class__.make(value=-expr.value, meta=self._make_node_meta(ctx))

        if op_name == "-":
            op_name = "neg"  # to tell apart from minus
        return nodes.Unary.make(name=op_name, expr=expr, meta=self._make_node_meta(ctx))

    def visitBinaryExpr(self, ctx: DataLensParser.BinaryExprContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        str_children, node_children = self._separate_children(ctx)
        op_name = "".join(str_children).lower()
        return nodes.Binary.make(
            name=op_name,
            left=node_children[0],
            right=node_children[1],
            meta=self._make_node_meta(ctx),
        )

    def visitBinaryExprSec(self, ctx: DataLensParser.BinaryExprContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        return self.visitBinaryExpr(ctx)

    def visitInExpr(self, ctx: DataLensParser.InExprContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        str_children, node_children = self._separate_children(ctx, exclude=",()")
        raw_exlist_nodes = [child for child in ctx.children if not isinstance(child, TerminalNodeImpl)][1:]
        op_name = "".join(str_children).lower()
        child_meta: Optional[nodes.NodeMeta] = None
        if raw_exlist_nodes:
            child_meta = self._make_node_meta(*raw_exlist_nodes)
        return nodes.Binary.make(
            name=op_name,
            left=node_children[0],
            right=nodes.ExpressionList.make(*node_children[1:], meta=child_meta),
            meta=self._make_node_meta(ctx),
        )

    def visitComparisonChain(self, ctx: DataLensParser.ComparisonChainContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        # First flatten comparison binary calls that get nested by the parser

        def is_cmp(_ctx) -> bool:  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
            return (
                len(_ctx.children) == 3
                and isinstance(_ctx.children[1], TerminalNodeImpl)
                and str(_ctx.children[1]) in COMPARISON_OP_RAW_NAMES
            )

        def _flatten_and_visit_cmp_ctx(
            _ctx: antlr4.ParserRuleContext,
        ) -> list[antlr4.ParserRuleContext]:
            if is_cmp(_ctx):
                return [
                    *_flatten_and_visit_cmp_ctx(_ctx.children[0]),
                    str(_ctx.children[1]),
                    self.visit(_ctx.children[2]),
                ]
            return [self.visit(_ctx)]

        args = _flatten_and_visit_cmp_ctx(ctx)

        if len(args) > 3:  # is a chain of more than one operator
            left, *middle_list, last_op, right = args
            middle_list = [
                [middle_list[i], middle_list[i + 1]] for i in range(0, len(middle_list), 2)
            ]  # repartition list into <operator, operand> pairs
            parts = middle_list + [(last_op, right)]
        elif len(args) == 3:
            left, last_op, right = args
            parts = [(last_op, right)]
        else:
            left, last_op = args
            right = nodes.Null()
            parts = [(last_op, right)]

        latest_pair = None
        latest_left = left
        for op_name, right in parts:
            if op_name == "=":
                op_name = "=="
            if op_name == "<>":
                op_name = "!="
            new_pair = nodes.Binary.make(
                name=op_name,
                left=latest_left,
                right=right,
                meta=self._make_node_meta(ctx),
            )
            if latest_pair is not None:
                new_pair = nodes.Binary.make(
                    name="and",
                    left=latest_pair,
                    right=new_pair,
                    meta=self._make_node_meta(ctx),
                )
            latest_left = right
            latest_pair = new_pair

        assert latest_pair is not None, "At least two operands are required in comparison chain"
        return latest_pair

    def visitUnaryPostfix(self, ctx: DataLensParser.UnaryPostfixContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        str_children, node_children = self._separate_children(ctx, lower_str=True)
        op_name = "".join([ch for ch in str_children if ch != "not"])
        expr = nodes.Unary.make(name=op_name, expr=node_children[0], meta=self._make_node_meta(ctx))
        if "not" in str_children:
            expr = nodes.Unary.make(name="not", expr=expr, meta=self._make_node_meta(ctx))
        return expr

    def visitBetweenExpr(self, ctx: DataLensParser.BetweenExprContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        str_children, node_children = self._separate_children(ctx, lower_str=True, exclude=("and",))
        op_name = "".join(str_children)
        return nodes.Ternary.make(
            name=op_name,
            first=node_children[0],
            second=node_children[1],
            third=node_children[2],
            meta=self._make_node_meta(ctx),
        )

    def visitParse(self, ctx: DataLensParser.ParseContext):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        if (
            len(ctx.children) > 0
            and isinstance(ctx.children[0], TerminalNodeImpl)
            and ctx.children[0].symbol.type == Token.EOF
        ):
            raise exc.ParseEmptyFormulaError(EMPTY_FORMULA_ERROR)
        return nodes.Formula.make(expr=self.visit(ctx.children[0]), meta=self._make_node_meta(ctx))
