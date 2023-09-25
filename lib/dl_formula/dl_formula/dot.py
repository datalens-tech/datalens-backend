from __future__ import annotations

from functools import singledispatchmethod
import textwrap
from typing import Union
import uuid

import graphviz

from dl_formula.core import nodes


def make_name() -> str:
    return "node_{}".format(uuid.uuid4().hex)


class DotTranslator:
    terminal_opts = dict(shape="rectangle", style="filled", margin="0")
    literal_colors = {
        str: "#fff6c1",
        int: "#b2dee6",
        float: "#c4b4e6",
        bool: "#d7e6df",
    }

    def __init__(self):
        pass

    def translate(self, formula: nodes.Formula, title: str = "Formula") -> graphviz.Digraph:
        """Translate ``Formula`` object into GraphViz DOT format"""
        dot = graphviz.Digraph(comment="Formula")
        self.translate_node(formula, dot)
        return dot

    def translate_node(self, node: nodes.FormulaItem, dot: graphviz.Digraph) -> str:
        return self._translate_node(node, dot)

    @singledispatchmethod
    def _translate_node(self, node: nodes.FormulaItem, dot: graphviz.Digraph) -> str:
        """Default action for methdispatch."""
        raise TypeError(type(node))

    @_translate_node.register(nodes.Formula)
    def _translate_node_formula(self, node: nodes.Formula, dot: graphviz.Digraph) -> str:
        node_name = make_name()
        wrapped_formula = "\n".join(textwrap.wrap(node.original_text or "", 90))
        dot.node(node_name, label=f"Formula:\n{wrapped_formula}", shape="plaintext")
        child_name = self.translate_node(node.expr, dot)
        dot.edge(node_name, child_name, label="EXPRESSION")
        return node_name

    @_translate_node.register(nodes.Field)
    def _translate_node_field(self, node: nodes.Field, dot: graphviz.Digraph) -> str:
        node_name = make_name()
        dot.node(node_name, label="[{}]".format(node.name), **self.terminal_opts, fillcolor="#e6cccc")
        return node_name

    @_translate_node.register(nodes.LiteralInteger)
    @_translate_node.register(nodes.LiteralFloat)
    @_translate_node.register(nodes.LiteralBoolean)
    @_translate_node.register(nodes.LiteralString)
    @_translate_node.register(nodes.LiteralDate)
    @_translate_node.register(nodes.LiteralDatetime)
    @_translate_node.register(nodes.LiteralGenericDatetime)
    @_translate_node.register(nodes.LiteralGeopoint)
    @_translate_node.register(nodes.LiteralGeopolygon)
    @_translate_node.register(nodes.LiteralUuid)
    def _translate_node_literal(self, node: nodes.BaseLiteral, dot: graphviz.Digraph) -> str:
        node_name = make_name()
        dot.node(
            node_name,
            label="{!r}".format(node.value),
            **self.terminal_opts,
            fillcolor=self.literal_colors[type(node.value)],
        )
        return node_name

    @_translate_node.register(nodes.Null)
    def _translate_node_null(self, node: nodes.Null, dot: graphviz.Digraph) -> str:
        node_name = make_name()
        dot.node(node_name, label="NULL", **self.terminal_opts, fillcolor="#959595")
        return node_name

    @_translate_node.register(nodes.ExpressionList)
    def _translate_node_expression_list(self, node: nodes.ExpressionList, dot: graphviz.Digraph) -> str:
        node_name = make_name()
        dot.node(
            node_name,
            label="[0..{}]".format(len(node.children) - 1),
            shape="hexagon",
            style="filled",
            fillcolor="#bba7fc",
        )
        for i, child in enumerate(node.children):  # type: ignore  # TODO: fix
            child_name = self.translate_node(child, dot)
            dot.edge(node_name, child_name, label="[{}]".format(i))

        return node_name

    @_translate_node.register(nodes.FuncCall)
    @_translate_node.register(nodes.WindowFuncCall)
    def _translate_node_func_call(
        self, node: Union[nodes.FuncCall, nodes.WindowFuncCall], dot: graphviz.Digraph
    ) -> str:
        node_name = make_name()
        args_names = ["ARG{}".format(i) for i in range(len(node.args))]
        dot.node(
            node_name,
            label="{}({})".format(node.name.upper(), ", ".join(args_names)),
            shape="oval",
            style="filled",
            fillcolor="#bde6b1",
        )
        for i, arg in enumerate(node.args):
            child_name = self.translate_node(arg, dot)
            dot.edge(node_name, child_name, label=args_names[i])

        if isinstance(node, nodes.WindowFuncCall):
            grouping_name = self.translate_node(node.grouping, dot)
            dot.edge(node_name, grouping_name, label="GROUPING")

            if list(node.ordering.expr_list):
                ordering_name = self.translate_node(node.ordering, dot)
                dot.edge(node_name, ordering_name, label="ORDERING")

            if list(node.before_filter_by.field_names):
                before_filter_by_name = self.translate_node(node.before_filter_by, dot)
                dot.edge(node_name, before_filter_by_name, label="...")

        return node_name

    @_translate_node.register(nodes.Binary)
    def _translate_node_binary(self, node: nodes.Binary, dot: graphviz.Digraph) -> str:
        node_name = make_name()
        op_name = node.name.upper()
        dot.node(node_name, label=op_name, shape="oval", style="filled", fillcolor="#c2c2c2")

        if op_name == "IN":
            left_label, right_label = "EL", "ARRAY"
        else:
            left_label, right_label = "LEFT", "RIGHT"

        left_name = self.translate_node(node.left, dot)
        dot.edge(node_name, left_name, label=left_label)
        right_name = self.translate_node(node.right, dot)
        dot.edge(node_name, right_name, label=right_label)
        return node_name

    @_translate_node.register(nodes.Ternary)
    def _translate_node_ternary(self, node: nodes.Ternary, dot: graphviz.Digraph) -> str:
        node_name = make_name()
        dot.node(node_name, label=node.name.upper(), shape="oval", style="filled", fillcolor="#c2c2c2")
        first_name = self.translate_node(node.first, dot)
        dot.edge(node_name, first_name, label="1ST")
        second_name = self.translate_node(node.second, dot)
        dot.edge(node_name, second_name, label="2ND")
        third_name = self.translate_node(node.third, dot)
        dot.edge(node_name, third_name, label="3RD")
        return node_name

    @_translate_node.register(nodes.Unary)
    def _translate_node_unary(self, node: nodes.Unary, dot: graphviz.Digraph) -> str:
        node_name = make_name()
        dot.node(node_name, label=node.name.upper(), shape="oval", style="filled", fillcolor="#c2c2c2")
        expr_name = self.translate_node(node.expr, dot)
        dot.edge(node_name, expr_name)
        return node_name

    @_translate_node.register(nodes.WindowGroupingTotal)
    def _translate_node_window_grouping_total(self, node: nodes.WindowGroupingTotal, dot: graphviz.Digraph) -> str:
        node_name = make_name()
        dot.node(node_name, label="TOTAL", shape="octagon", style="filled", fillcolor="#bba7fc")
        return node_name

    @_translate_node.register(nodes.WindowGroupingWithin)
    @_translate_node.register(nodes.WindowGroupingAmong)
    def _translate_node_window_grouping_with_dimensions(
        self, node: Union[nodes.WindowGroupingWithin, nodes.WindowGroupingAmong], dot: graphviz.Digraph
    ) -> str:
        node_name = make_name()
        keyword = {
            nodes.WindowGroupingWithin: "WHITHIN",
            nodes.WindowGroupingAmong: "AMONG",
        }[type(node)]
        dot.node(node_name, label=keyword, shape="octagon", style="filled", fillcolor="#ff898b")
        for i, child in enumerate(node.children):
            child_name = self.translate_node(child, dot)  # type: ignore  # TODO: fix
            dot.edge(node_name, child_name, label="DIM {}".format(i))

        return node_name

    @_translate_node.register(nodes.Ordering)
    def _translate_node_ordering(self, node: nodes.Ordering, dot: graphviz.Digraph) -> str:
        node_name = make_name()
        dot.node(node_name, label="ORDER BY", shape="octagon", style="filled", fillcolor="#ff898b")
        for i, child in enumerate(node.children):  # type: ignore  # TODO: fix
            child_name = self.translate_node(child, dot)
            dot.edge(node_name, child_name, label="EXPR {}".format(i))

        return node_name

    @_translate_node.register(nodes.BeforeFilterBy)
    def _translate_node_before_filter_by(self, node: nodes.BeforeFilterBy, dot: graphviz.Digraph) -> str:
        node_name = make_name()
        dot.node(node_name, label="BEFORE FILTER BY", shape="octagon", style="filled", fillcolor="#ff898b")
        for i, field_name in enumerate(node.field_names):
            child_name = self.translate_node(nodes.Field.make(name=field_name), dot)
            dot.edge(node_name, child_name, label="FIELD {}".format(i))

        return node_name

    @_translate_node.register(nodes.IfBlock)
    def _translate_node_if_block(self, node: nodes.IfBlock, dot: graphviz.Digraph) -> str:
        root_name = make_name()
        dot.node(root_name, label="IF-BLOCK", shape="pentagon", style="filled", fillcolor="#c2c2c2")

        parent_name = None
        for if_part in node.if_list:
            if_name = make_name()
            dot.node(if_name, label="IF", shape="diamond", style="filled", fillcolor="#c2c2c2")
            dot.edge(root_name, if_name, label="PART")
            if parent_name:
                dot.edge(parent_name, if_name, label="ELSE", style="dotted")
            cond_name = self.translate_node(if_part.cond, dot)
            dot.edge(if_name, cond_name, label="TRUE?")
            expr_name = self.translate_node(if_part.expr, dot)
            dot.edge(if_name, expr_name, label="THEN")
            parent_name = if_name

        else_name = self.translate_node(node.else_expr, dot)
        dot.edge(parent_name, else_name, label="ELSE", style="dotted")
        dot.edge(root_name, else_name, label="PART")

        return root_name

    @_translate_node.register(nodes.CaseBlock)
    def _translate_node_case_block(self, node: nodes.CaseBlock, dot: graphviz.Digraph) -> str:
        root_name = make_name()
        dot.node(root_name, label="CASE", shape="pentagon", style="filled", fillcolor="#c2c2c2")
        expr_name = self.translate_node(node.case_expr, dot)
        dot.edge(root_name, expr_name, label="WHAT")

        for when_part in node.when_list:
            when_name = make_name()
            dot.node(when_name, label="WHEN", shape="diamond", style="filled", fillcolor="#c2c2c2")
            dot.edge(root_name, when_name)
            value_name = self.translate_node(when_part.val, dot)
            dot.edge(when_name, value_name, label="IS")
            expr_name = self.translate_node(when_part.expr, dot)
            dot.edge(when_name, expr_name, label="THEN")

        else_name = self.translate_node(node.else_expr, dot)
        dot.edge(root_name, else_name, label="ELSE")

        return root_name

    @_translate_node.register(nodes.ParenthesizedExpr)
    @_translate_node.register(nodes.OrderAscending)
    @_translate_node.register(nodes.OrderDescending)
    def _translate_node_parenthesized_expr(self, node: nodes.ParenthesizedExpr, dot: graphviz.Digraph) -> str:
        node_name = make_name()
        label = {
            nodes.ParenthesizedExpr: "(...)",
            nodes.OrderAscending: "ASC",
            nodes.OrderDescending: "DESC",
        }[type(node)]
        dot.node(node_name, label=label, shape="oval", style="filled", fillcolor="#e0e0e0")
        expr_name = self.translate_node(node.expr, dot)
        dot.edge(node_name, expr_name, label="EXPR")
        return node_name


def translate(formula: nodes.Formula, title: str = "Formula") -> graphviz.Digraph:
    """Translate formula into DOT representation"""
    translator = DotTranslator()
    return translator.translate(formula, title=title)
