from __future__ import annotations

from enum import Enum
from typing import (
    ClassVar,
    Sequence,
    Set,
)

from dl_formula.core.datatype import DataType
from dl_formula.core.dialect import StandardDialect as D
from dl_formula.definitions.args import (
    ArgTypeForAll,
    ArgTypeSequence,
)
from dl_formula.definitions.base import (
    Function,
    TranslationVariant,
    TranslationVariantWrapped,
)
from dl_formula.definitions.operators_binary import BinaryPlus
from dl_formula.definitions.scope import Scope
from dl_formula.definitions.type_strategy import Fixed
from dl_formula.shortcuts import n


V = TranslationVariant.make
VW = TranslationVariantWrapped.make

MARKUP_EFFECTIVELY = DataType.MARKUP.autocast_types | DataType.STRING.autocast_types

MARK_LPAR = "("
MARK_RPAR = ")"
MARK_SEP = " "
MARK_QUOT = '"'


class StrictMarkupCompatibleArgTypes(ArgTypeForAll):
    """Markup-compatible arguments with at least one markup argument"""

    __slots__ = ()

    _compatible_types = MARKUP_EFFECTIVELY
    _required_types = DataType.MARKUP.autocast_types

    def __init__(self) -> None:
        super().__init__(expected_types=MARKUP_EFFECTIVELY)

    def match_arg_types(self, arg_types: Sequence[DataType]) -> bool:
        if not super().match_arg_types(arg_types):
            return False

        return bool(self._required_types & set(arg_types))

    def get_possible_arg_types_at_pos(self, pos: int, total: int) -> Set[DataType]:
        return set(MARKUP_EFFECTIVELY)


# TODO: make this definitely unnecessary.
def concat_strings(items, sep=""):  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation  [no-untyped-def]
    result = []
    literals_buf = []  # type: ignore  # TODO: fix

    def flush():  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        if literals_buf:
            result.append(sep.join(literals_buf))
            literals_buf[:] = []

    for item in items:
        if isinstance(item, str):
            literals_buf.append(item)
        else:
            flush()
            result.append(item)
    flush()
    return result


class FuncInternalStrBase(Function):
    """
    Base class for a helper for converting markup to str internally.
    """

    name = "__str"
    scopes = Function.scopes & ~Scope.EXPLICIT_USAGE & ~Scope.SUGGESTED & ~Scope.DOCUMENTED

    arg_names = ["expression"]
    arg_cnt = 1

    # Markup is already a string value, so nothing to do.
    variants = [
        V(D.DUMMY | D.SQLITE, lambda value: value),
    ]


class FuncInternalStrConst(FuncInternalStrBase):
    argument_types = [
        ArgTypeSequence([{DataType.CONST_MARKUP, DataType.CONST_STRING}]),
    ]
    return_type = Fixed(DataType.CONST_STRING)


class FuncInternalStr(FuncInternalStrBase):
    argument_types = [
        ArgTypeSequence([MARKUP_EFFECTIVELY]),
    ]
    return_type = Fixed(DataType.STRING)


def concat(nodes):  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation  [no-untyped-def]
    nodes = concat_strings(nodes)
    nodes_processed = [n.func.__STR(node) for node in nodes]

    if len(nodes_processed) == 1:
        return nodes_processed[0]

    return n.func.CONCAT(*nodes_processed)


def process_markup_child(node):  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation  [no-untyped-def]
    if node.data_type == DataType.MARKUP:
        return node
    if node.data_type == DataType.CONST_MARKUP:
        return node
    if node.data_type == DataType.STRING:
        pieces = [MARK_QUOT, n.func.REPLACE(node, MARK_QUOT, MARK_QUOT + MARK_QUOT), MARK_QUOT]
        return concat(pieces)
    if node.data_type == DataType.INTEGER:
        return n.func.CONCAT(MARK_QUOT, n.func.STR(node), MARK_QUOT)
    # XXXX: Should probaly be handled automatically by the `STRING` case above.
    if node.data_type == DataType.CONST_STRING or node.data_type == DataType.CONST_INTEGER:
        value = node.expression.value
        return "".join((MARK_QUOT, str(value).replace(MARK_QUOT, MARK_QUOT + MARK_QUOT), MARK_QUOT))
    if node.data_type == DataType.NULL and node.base_token == "image":
        return '""'
    if node.data_type == DataType.NULL:
        return node
    raise Exception("Unexpected markup child type", node)


def markup_node(func_name, *children):  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation  [no-untyped-def]
    """
    Using `concat` and `replace`, return an expression that results in
    something parseable by the special parser.
    """
    pieces = [
        MARK_LPAR,
        func_name,
    ]
    for child in children:
        child_res = process_markup_child(child)
        pieces += [MARK_SEP, child_res]
    pieces += [MARK_RPAR]
    return concat(pieces)


def make_variants(func_name):  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation  [no-untyped-def]
    return [
        VW(
            D.DUMMY | D.SQLITE,
            lambda *children: markup_node(func_name, *children),
        ),
    ]


class MarkupTypeStrategy(Fixed):
    """
    Consts -> CONST_MARKUP, otherwise non-const MARKUP.

    Inherits `Fixed` for docgen.
    """

    def __init__(self) -> None:
        super().__init__(DataType.MARKUP)

    def get_from_args(self, arg_types):  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation  [no-untyped-def]
        if all(arg.casts_to(DataType.CONST_MARKUP) or arg.casts_to(DataType.CONST_STRING) for arg in arg_types):
            return DataType.CONST_MARKUP
        return DataType.MARKUP


class FuncMarkup(Function):
    return_type = MarkupTypeStrategy()


class FuncMarkupUnary(FuncMarkup):
    # required: name
    # required: variants

    # optional: description
    # optional: notes
    # optional: examples

    # TODO: make all unary markup functions accept an arbitrary amount of
    # arguments, passing them to concat internally.

    arg_cnt = 1
    arg_names = ["text"]
    argument_types = [
        ArgTypeSequence([MARKUP_EFFECTIVELY]),
    ]


class FuncBold(FuncMarkupUnary):
    name = "bold"
    variants = make_variants("b")


class FuncItalics(FuncMarkupUnary):
    name = "italic"
    variants = make_variants("i")


class FuncUrl(FuncMarkup):
    name = "url"
    arg_cnt = 2
    arg_names = ["address", "text"]
    argument_types = [
        # url(str, markup|str)
        ArgTypeSequence([DataType.STRING, MARKUP_EFFECTIVELY]),
    ]
    variants = make_variants("a")


class BinaryPlusMarkup(BinaryPlus):
    variants = make_variants("c")
    argument_types = [
        StrictMarkupCompatibleArgTypes(),
    ]
    return_type = Fixed(DataType.MARKUP)


class ConcatMultiMarkup(FuncMarkup):
    """
    User-facing 'concat' of markups.
    Also useful for turning a string into markup,
    e.g. `if cond then italics(col) else markup(col) end` case.
    """

    name = "markup"
    arg_cnt = None
    argument_types = [
        ArgTypeForAll(MARKUP_EFFECTIVELY),
    ]
    return_type = Fixed(DataType.MARKUP)  # type: ignore  # TODO: fix

    variants = make_variants("c")


class FuncColor(FuncMarkup):
    name = "color"
    arg_cnt = 2
    arg_names = ["text", "color"]
    argument_types = [
        # color( markup|str, str)
        ArgTypeSequence([MARKUP_EFFECTIVELY, DataType.STRING]),
    ]
    variants = make_variants("cl")


class FuncSize(FuncMarkup):
    name = "size"
    arg_cnt = 2
    arg_names = ["text", "size"]
    argument_types = [
        # size( markup|str, str)
        ArgTypeSequence([MARKUP_EFFECTIVELY, DataType.STRING]),
    ]
    variants = make_variants("sz")


class FuncBr(FuncMarkup):
    name = "br"
    arg_cnt = 0
    variants = make_variants("br")


class NullImageMarkupNode:
    data_type: ClassVar[Enum] = DataType.NULL
    base_token: ClassVar[str] = "image"


null_image_node = NullImageMarkupNode()


class FuncImageBase(FuncMarkup):
    name = "image"


class FuncImage1(FuncImageBase):
    arg_cnt = 1
    arg_names = ["src"]
    argument_types = [
        # image( null|str)
        ArgTypeSequence(
            [
                DataType.STRING.autocast_types | DataType.NULL.autocast_types,
            ]
        ),
    ]
    variants = [
        VW(
            D.DUMMY | D.SQLITE,
            lambda src: markup_node("img", src, null_image_node, null_image_node, null_image_node),
        ),
    ]


class FuncImage2(FuncImageBase):
    arg_cnt = 2
    arg_names = ["src", "width"]
    argument_types = [
        # image( null|str, null|int)
        ArgTypeSequence(
            [
                DataType.STRING.autocast_types | DataType.NULL.autocast_types,
                DataType.INTEGER.autocast_types | DataType.CONST_INTEGER.autocast_types | DataType.NULL.autocast_types,
            ]
        ),
    ]
    variants = [
        VW(
            D.DUMMY | D.SQLITE,
            lambda src, width: markup_node("img", src, width, null_image_node, null_image_node),
        ),
    ]


class FuncImage3(FuncImageBase):
    arg_cnt = 3
    arg_names = ["src", "width", "height"]
    argument_types = [
        # image( null|str, null|int, null|int)
        ArgTypeSequence(
            [
                DataType.STRING.autocast_types | DataType.NULL.autocast_types,
                DataType.INTEGER.autocast_types | DataType.CONST_INTEGER.autocast_types | DataType.NULL.autocast_types,
                DataType.INTEGER.autocast_types | DataType.CONST_INTEGER.autocast_types | DataType.NULL.autocast_types,
            ]
        ),
    ]
    variants = [
        VW(
            D.DUMMY | D.SQLITE,
            lambda src, width, height: markup_node("img", src, width, height, null_image_node),
        ),
    ]


class FuncImage4(FuncImageBase):
    arg_cnt = 4
    arg_names = ["src", "width", "height", "alt"]
    argument_types = [
        # image( null|str, null|int, null|int, null|str)
        ArgTypeSequence(
            [
                DataType.STRING.autocast_types | DataType.NULL.autocast_types,
                DataType.INTEGER.autocast_types | DataType.CONST_INTEGER.autocast_types | DataType.NULL.autocast_types,
                DataType.INTEGER.autocast_types | DataType.CONST_INTEGER.autocast_types | DataType.NULL.autocast_types,
                DataType.STRING.autocast_types | DataType.NULL.autocast_types,
            ]
        ),
    ]
    variants = make_variants("img")


DEFINITIONS_MARKUP = [
    # +
    BinaryPlusMarkup,
    # __str
    FuncInternalStrConst,
    FuncInternalStr,
    # bold
    FuncBold,
    # italic
    FuncItalics,
    # markup
    ConcatMultiMarkup,
    # url
    FuncUrl,
    # size
    FuncSize,
    # color
    FuncColor,
    # br
    FuncBr,
    # image
    FuncImage1,
    FuncImage2,
    FuncImage3,
    FuncImage4,
]
