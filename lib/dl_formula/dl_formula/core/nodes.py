from __future__ import annotations

import abc
import copy
import datetime
from typing import (
    Any,
    Callable,
    ClassVar,
    Collection,
    Generator,
    Generic,
    Hashable,
    Iterable,
    Mapping,
    Optional,
    Sequence,
    TypeVar,
    Union,
    cast,
)
import uuid

from dl_formula.core.extract import NodeExtract
from dl_formula.core.index import NodeHierarchyIndex
from dl_formula.core.position import Position
from dl_formula.core.tag import LevelTag


class NodeMeta:
    __slots__ = ("position", "original_text", "level_tag")

    def __init__(
        self,
        position: Optional[Position] = None,
        original_text: Optional[str] = None,
        level_tag: Optional[LevelTag] = None,
    ):
        self.position = position or Position()
        self.original_text = original_text
        self.level_tag = level_tag

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(position={self.position!r})"

    def __str__(self) -> str:
        return repr(self)

    def with_tag(self, level_tag: LevelTag) -> NodeMeta:
        """
        Return a copy of meta with replaced ``self.level_tag``
        """
        return self.__class__(
            position=self.position,
            original_text=self.original_text,
            level_tag=level_tag,
        )


_FORMULA_ITEM_TV = TypeVar("_FORMULA_ITEM_TV", bound="FormulaItem")


class FormulaItem(abc.ABC):
    """Abstract class. Base class for all formula building blocks."""

    __slots__ = ("__children", "__internal_value", "__meta", "__extract")
    show_names: tuple[str, ...] = ()
    autonomous: ClassVar[bool] = True
    # If True, then the node can represent a standalone expression.
    # However, some nodes are can only exist as an integral part of a parent node (sub-clauses).

    @property
    def children(self) -> tuple[FormulaItem, ...]:
        return tuple(self.__children)

    @property
    def internal_value(self) -> tuple[Optional[Hashable], ...]:
        return self.__internal_value

    @property
    def position(self) -> Position:
        return self.__meta.position

    @property
    def original_text(self) -> Optional[str]:
        return self.__meta.original_text

    @property
    def level_tag(self) -> Optional[LevelTag]:
        return self.__meta.level_tag

    @property
    def pos_range(self) -> tuple[Optional[int], Optional[int]]:
        return self.__meta.position.start, self.__meta.position.end

    @property
    def meta(self) -> NodeMeta:
        return self.__meta

    def __init__(
        self,
        *children: FormulaItem,
        internal_value: tuple[Optional[Hashable], ...] = (),
        meta: Optional[NodeMeta] = None,
    ):
        for child in children:
            if not isinstance(child, FormulaItem):
                raise TypeError(f"Invalid type {type(child)} for child node")

        self.validate_children(children)
        assert isinstance(internal_value, tuple)
        self.validate_internal_value(internal_value)

        self.__children: list[FormulaItem] = [child for child in children]
        self.__internal_value = internal_value
        self.__meta: NodeMeta = meta or NodeMeta()
        self.__extract = self._make_extract()

    @classmethod
    def validate_children(cls, children: Sequence[FormulaItem]) -> None:
        return

    @classmethod
    def validate_internal_value(cls, internal_value: tuple[Optional[Hashable], ...]) -> None:
        assert internal_value == ()

    def get_scalar_value(self) -> Optional[Hashable]:
        return self.__internal_value

    def stringify(self, with_meta: bool = False) -> str:
        show_names = self.show_names if not with_meta else (self.show_names + ("meta",))
        kv_pairs = []
        for name in show_names:
            if name == "meta":
                attr_value = self.meta
            else:
                attr_value = getattr(self, name)
            if isinstance(attr_value, FormulaItem):
                attr_value_s = attr_value.stringify(with_meta=with_meta)
            else:
                attr_value_s = repr(attr_value)
            pair = f"{name}={attr_value_s}"
            kv_pairs.append(pair)
        param_str = ", ".join(kv_pairs)
        return "{}({})".format(self.__class__.__name__, param_str)

    def __repr__(self) -> str:
        return self.stringify()

    def __str__(self) -> str:
        return repr(self)

    def __eq__(self, other: Any) -> bool:
        return other.__class__ is self.__class__ and self.__extract == other.__extract

    def pretty(
        self,
        indent: str = "  ",
        initial_indent: str = "",
        inline_len: int = 70,
        do_leading_indent: bool = False,
        with_meta: bool = False,
    ) -> str:
        """Return a "pretty" str version of the Node tree: multiple lines with indentations"""
        str_version = self.stringify(with_meta=with_meta)
        if len(str_version) <= inline_len:
            return str_version

        def prettify_child(_child: Any, _initial_indent: str) -> str:
            if isinstance(_child, FormulaItem):
                return _child.pretty(
                    initial_indent=_initial_indent,
                    indent=indent,
                    inline_len=inline_len,
                    do_leading_indent=False,
                    with_meta=with_meta,
                )
            if isinstance(_child, tuple):
                if not _child:
                    return "()"
                res = "(\n"
                item_indent = _initial_indent + indent
                for item in _child:
                    res += "{}{},\n".format(item_indent, prettify_child(item, item_indent))
                res += _initial_indent + ")"
                return res
            else:
                return repr(_child)

        lines = []
        lines.append("{}{}(".format(initial_indent if do_leading_indent else "", self.__class__.__name__))
        show_names = self.show_names if not with_meta else (self.show_names + ("meta",))
        for name in show_names:
            if name == "meta":
                child = str(self.__meta)
            else:
                child = prettify_child(getattr(self, name), initial_indent + indent)
            lines.append("{}{}={},".format(initial_indent + indent, name, child))
        lines.append("{})".format(initial_indent))

        return "\n".join(lines)

    def visit_node_type(self, node_type: type[FormulaItem], visit_func: Callable[[FormulaItem], Any]) -> None:
        """Walk the whole ``FormulaItem`` (sub)tree and call ``visit_func`` for all nodes of given type"""
        if isinstance(self, node_type):
            visit_func(self)

        for child in self.__children:
            child.visit_node_type(node_type=node_type, visit_func=visit_func)

    def list_node_type(self, node_type: type[_FORMULA_ITEM_TV]) -> list[_FORMULA_ITEM_TV]:
        res: list[_FORMULA_ITEM_TV] = []
        self.visit_node_type(node_type=node_type, visit_func=res.append)  # type: ignore  # 2024-01-30 # TODO: Argument "visit_func" to "visit_node_type" of "FormulaItem" has incompatible type "Callable[[_FORMULA_ITEM_TV], None]"; expected "Callable[[FormulaItem], Any]"  [arg-type]
        return res

    def get_by_pos(
        self,
        pos: int,
        node_types: Optional[tuple[type[FormulaItem], ...]] = None,
    ) -> Optional[FormulaItem]:
        """
        Return the innermost node at position ``pos``.
        If ``node_types`` is given, then choose only among nodes of the listed types.
        """
        if self.position.start is None or self.position.end is None:
            return None

        if self.position.start <= pos <= self.position.end:  # include both ends
            # self fits the position
            # try to narrow down the result  by inspecting children
            matching_children = []
            for child in self.__children:
                res = child.get_by_pos(pos, node_types=node_types)
                if res is not None:
                    matching_children.append(res)

            if matching_children:
                # always prefer the rightmost of the matches
                return matching_children[-1]

            if node_types is None or isinstance(self, node_types):
                return self

        return None

    @staticmethod
    def __inplace_replace_at_index_no_copy(
        top_node: _FORMULA_ITEM_TV, index: NodeHierarchyIndex, expr: FormulaItem
    ) -> None:
        parent_index, child_index = index.rsplit()
        assert child_index is not None
        parent = top_node.get(parent_index)

        parent.__children[child_index] = expr
        for node in reversed(list(top_node.iter_index(parent_index))):
            node.__extract = node._make_extract()

    def substitute_batch(
        self: _FORMULA_ITEM_TV, to_substitute: Mapping[NodeHierarchyIndex, FormulaItem]
    ) -> _FORMULA_ITEM_TV:
        """
        Substitutes sub nodes by given indexes
        Note that substituent nodes are not copied before "insertion"
        """
        self_copy: _FORMULA_ITEM_TV = copy.copy(self)
        for index, expr in to_substitute.items():
            self.__inplace_replace_at_index_no_copy(self_copy, index=index, expr=expr)

        self_copy.__extract = self_copy._make_extract()
        return self_copy

    def replace_at_index(self: _FORMULA_ITEM_TV, index: NodeHierarchyIndex, expr: FormulaItem) -> _FORMULA_ITEM_TV:
        """
        Replace node at index ``index`` with a new one specified by ``expr``.
        Creating new nodes while descending along the `index` in recursion.
        """
        child_in_index, index_tail = index.lsplit()

        # final replacement
        if child_in_index is None:
            return expr  # type: ignore  # 2024-01-30 # TODO: Incompatible return value type (got "FormulaItem", expected "_FORMULA_ITEM_TV")  [return-value]

        child = self.__children[child_in_index]
        new_child = child.replace_at_index(index_tail, expr)

        children = self.__children[:]
        children[child_in_index] = new_child

        return self.light_copy(children)

    def light_copy(
        self: _FORMULA_ITEM_TV, children: Sequence[FormulaItem], *, meta: Optional[NodeMeta] = None
    ) -> _FORMULA_ITEM_TV:
        """
        Creates a copy of self with new children nodes
        """

        return self.__class__(
            *children,
            internal_value=self.__internal_value,
            meta=meta or self.__meta,
        )

    def replace_nodes(
        self: _FORMULA_ITEM_TV,
        match_func: Callable[[FormulaItem, tuple[FormulaItem, ...]], bool],
        replace_func: Callable[[FormulaItem, tuple[FormulaItem, ...]], FormulaItem],
        parent_stack: tuple[FormulaItem, ...] = (),
    ) -> _FORMULA_ITEM_TV:
        """
        Walk the whole ``FormulaItem`` (sub)tree and replace child nodes
        for which ``replace_func(<child_node>)`` is ``True`` with``replace_func(<child_node>)``.
        Return a copy of the original node with replacements if any occurred, otherwise returns self.
        """
        if not self.__children:
            return self

        is_modified = False

        parent_stack_w_self = parent_stack + (self,)
        to_replace: dict[int, FormulaItem] = {}

        for idx, child in enumerate(self.__children):
            modified_child = child.replace_nodes(match_func, replace_func, parent_stack_w_self)
            if modified_child is not child or modified_child != child:
                child = to_replace[idx] = modified_child
                is_modified = True

            if match_func(child, parent_stack_w_self):
                modified_child = replace_func(child, parent_stack_w_self)
                if modified_child is not child or modified_child != child:
                    to_replace[idx] = modified_child
                    is_modified = True

        if is_modified:
            children = cast(
                list[FormulaItem],
                [to_replace[idx] if idx in to_replace else child for idx, child in enumerate(self.__children)],
            )
            return self.light_copy(children)

        return self

    def __copy__(self: _FORMULA_ITEM_TV) -> _FORMULA_ITEM_TV:
        """Create a copy of self"""

        return self.__class__(
            *[copy.copy(c) for c in self.__children],
            internal_value=self.__internal_value,
            meta=self.__meta,
        )

    def __inplace_replace_meta(self, meta: NodeMeta) -> None:
        self.__meta = meta
        self.__extract = self._make_extract()

    def with_tag(self: _FORMULA_ITEM_TV, level_tag: LevelTag) -> _FORMULA_ITEM_TV:
        """
        Return a copy of node with replaced ``self.meta.level_tag``
        """
        return self.light_copy(self.children[:], meta=self.meta.with_tag(level_tag=level_tag))

    def get(self, index: NodeHierarchyIndex) -> FormulaItem:
        """Find and return sub-node identified by ``index``"""
        head, tail = index.lsplit()
        if head is None:
            return self
        return self.__children[head].get(tail)

    def __getitem__(self, item: NodeHierarchyIndex) -> FormulaItem:
        return self.get(item)

    def enumerate(
        self,
        prefix: NodeHierarchyIndex = NodeHierarchyIndex(),  # noqa: B008
        max_depth: Optional[int] = None,
    ) -> Generator[tuple[NodeHierarchyIndex, FormulaItem], None, None]:
        """
        Similar to the standard ``enumerate`` function,
        but iterates over the node hierarchy
        and uses ``NodeHierarchyIndex`` instead of an integer index.

        Recursion can be limited by ``max_depth``.
        """
        yield prefix, self
        if max_depth == 0:
            return
        child_max_depth = max_depth - 1 if max_depth is not None else None
        for child_i, child in enumerate(self.children):
            yield from child.enumerate(prefix=prefix + child_i, max_depth=child_max_depth)

    def resolve_index(self, node: FormulaItem, pointer_eq: bool = True) -> Optional[NodeHierarchyIndex]:
        """Get index for given ``node``."""
        for index, child in self.enumerate():
            if pointer_eq:
                if child is node:
                    return index
            elif child == node:  # (and not pointer_eq)
                return index

        raise ValueError("Cannot resolve index for node")

    def iter_index(self, index: NodeHierarchyIndex, exclude_last: bool = False) -> Generator[FormulaItem, None, None]:
        """Iterate over all ancestor nodes (and, optionally, the target child node)."""
        node = self
        yield self
        max_i = len(index.indices) - 1
        for hierarchy_level, flat_ind in enumerate(index.indices):
            node = node.__children[flat_ind]
            if not exclude_last or hierarchy_level < max_i:
                yield node

    @property
    def start_pos(self) -> Optional[int]:
        return self.position.start

    @property
    def end_pos(self) -> Optional[int]:
        return self.position.end

    @property
    def complexity(self) -> Optional[int]:
        return self.__extract.complexity if self.__extract is not None else None

    def _make_extract(self) -> Optional[NodeExtract]:
        """
        Return a hashable immutable object that represents this node and its children.
        It should not contain any syntactic info so that can be used for optimization of data calculations.
        """
        optional_child_extracts = tuple(child.extract for child in self.children)
        if None in optional_child_extracts:
            return None
        child_extracts = cast(tuple[NodeExtract, ...], optional_child_extracts)
        return NodeExtract(
            type_name=type(self).__name__,
            children=child_extracts,
            complexity=sum(ce.complexity if ce is not None else 0 for ce in child_extracts) + 1,
            value=self.get_scalar_value(),
        )

    @property
    def extract(self) -> Optional[NodeExtract]:
        return self.__extract

    @property
    def extract_not_none(self) -> NodeExtract:
        assert self.__extract is not None
        return self.__extract


class Child(Generic[_FORMULA_ITEM_TV]):
    """Descriptor for node properties that refer to child nodes"""

    __slots__ = ("ind",)

    def __init__(self, ind: int):
        self.ind = ind

    def __get__(self, instance: FormulaItem, owner: type[FormulaItem]) -> _FORMULA_ITEM_TV:
        if instance is None:
            raise TypeError("Cannot be used on class")
        return instance.children[self.ind]  # type: ignore  # Cannot validate type here

    def __set__(self, instance: FormulaItem, value: Any) -> None:
        raise RuntimeError("Cannot set children")


class MultiChild(Generic[_FORMULA_ITEM_TV]):
    """Descriptor for node properties that refer to child nodes"""

    __slots__ = ("slice",)

    def __init__(self, slice: slice):
        self.slice = slice

    def __get__(self, instance: FormulaItem, owner: type[FormulaItem]) -> tuple[_FORMULA_ITEM_TV, ...]:
        if instance is None:
            raise TypeError("Cannot be used on class")
        return instance.children[self.slice]  # type: ignore  # Cannot validate type here

    def __set__(self, instance: FormulaItem, value: Any) -> None:
        raise RuntimeError("Cannot set children")


class Null(FormulaItem):
    """Database NULL value"""

    __slots__ = ()

    @classmethod
    def validate_children(cls, children: Sequence[FormulaItem]) -> None:
        assert not children, "Null does not accept children"


class ExprWrapper(FormulaItem):
    __slots__ = ()
    show_names = FormulaItem.show_names + ("expr",)

    expr: Child[FormulaItem] = Child(0)

    @classmethod
    def validate_children(cls, children: Sequence[FormulaItem]) -> None:
        assert len(children) == 1

    @classmethod
    def make(cls: type[_FORMULA_ITEM_TV], expr: FormulaItem, *, meta: Optional[NodeMeta] = None) -> _FORMULA_ITEM_TV:
        return cls(expr, meta=meta)


class ParenthesizedExpr(ExprWrapper):
    """Represents the wrapping of an expression into parentheses"""

    __slots__ = ()


_LITERAL_TV = TypeVar("_LITERAL_TV", bound="BaseLiteral")


class BaseLiteral(FormulaItem):
    """Represents a constant value"""

    __slots__ = ()
    show_names = FormulaItem.show_names + ("value",)

    @property
    def value(self) -> Any:
        return self.internal_value[0]

    @classmethod
    def validate_children(cls, children: Sequence[FormulaItem]) -> None:
        assert len(children) == 0

    @classmethod
    def validate_internal_value(cls, internal_value: tuple[Optional[Hashable], ...]) -> None:
        assert len(internal_value) == 1
        cls.validate_literal_value(internal_value[0])

    @classmethod
    def validate_literal_value(cls, literal_value: Optional[Hashable]) -> None:
        pass

    @classmethod
    def make(cls: type[_LITERAL_TV], value: Any, *, meta: Optional[NodeMeta] = None) -> _LITERAL_TV:
        internal_value = (value,)
        return cls(internal_value=internal_value, meta=meta)


class Array:
    __slots__ = ("item_cls",)

    def __init__(self, item_cls: type):
        self.item_cls = item_cls

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.item_cls})"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.item_cls})"


def does_type_match(
    value: Any,
    expected_type: Union[type, tuple[Union[type, Array], ...], Array],
) -> bool:
    if isinstance(expected_type, Array):
        if not isinstance(value, list):
            return False
        for item in value:
            if item is None or isinstance(item, Null):
                # null values are supported in arrays
                continue
            if not does_type_match(item, expected_type=expected_type.item_cls):
                return False
        return True
    elif isinstance(expected_type, tuple):
        return any(does_type_match(value, expected_type=sub_type) for sub_type in expected_type)
    elif isinstance(expected_type, type):
        return isinstance(value, expected_type)

    raise TypeError(f"Unexpected type: {expected_type}")


def check_type(value: Any, expected_type: Union[type, tuple[Union[type, Array], ...], Array]) -> None:
    if not does_type_match(value, expected_type):
        raise TypeError(f"Expected type {expected_type}, got {type(value)}")


class LiteralInteger(BaseLiteral):
    """Represents an INTEGER literal"""

    __slots__ = ()

    @classmethod
    def validate_literal_value(cls, literal_value: Optional[Hashable]) -> None:
        assert isinstance(literal_value, int)


class LiteralFloat(BaseLiteral):
    """Represents a FLOAT literal"""

    __slots__ = ()

    @classmethod
    def validate_literal_value(cls, literal_value: Optional[Hashable]) -> None:
        assert isinstance(literal_value, float)


class LiteralBoolean(BaseLiteral):
    """Represents a BOOLEAN literal"""

    __slots__ = ()

    @classmethod
    def validate_literal_value(cls, literal_value: Optional[Hashable]) -> None:
        assert isinstance(literal_value, bool)


class LiteralString(BaseLiteral):
    """Represents a STRING literal"""

    __slots__ = ()

    @classmethod
    def validate_literal_value(cls, literal_value: Optional[Hashable]) -> None:
        assert isinstance(literal_value, str)


class LiteralDate(BaseLiteral):
    """Represents a DATE literal"""

    __slots__ = ()

    @classmethod
    def validate_literal_value(cls, literal_value: Optional[Hashable]) -> None:
        assert isinstance(literal_value, datetime.date)


class LiteralDatetime(BaseLiteral):
    """Represents a DATETIME literal"""

    __slots__ = ()

    @classmethod
    def validate_literal_value(cls, literal_value: Optional[Hashable]) -> None:
        assert isinstance(literal_value, datetime.datetime)


class LiteralDatetimeTZ(BaseLiteral):
    """Represents a DATETIMETZ literal"""

    __slots__ = ()

    @classmethod
    def validate_literal_value(cls, literal_value: Optional[Hashable]) -> None:
        assert isinstance(literal_value, datetime.datetime)
        if literal_value.tzinfo is None:
            raise TypeError("Expected non-empty datetime tzinfo")


class LiteralGenericDatetime(BaseLiteral):
    """Represents a GENERICDATETIME literal"""

    __slots__ = ()

    @classmethod
    def validate_literal_value(cls, literal_value: Optional[Hashable]) -> None:
        assert isinstance(literal_value, datetime.datetime)


class LiteralGeopoint(LiteralString):
    """Represents a GEOPOINT literal"""

    __slots__ = ()


class LiteralGeopolygon(LiteralString):
    """Represents a GEOPOLYGON literal"""

    __slots__ = ()


# # Should *probably* not be done: LiteralMarkup


class LiteralUuid(BaseLiteral):
    """Represents a UUID literal"""

    __slots__ = ()

    @classmethod
    def make(cls, value: Union[uuid.UUID, str], *, meta: Optional[NodeMeta] = None) -> LiteralUuid:
        value = value if isinstance(value, uuid.UUID) else uuid.UUID(value)
        return super().make(value=value, meta=meta)

    @classmethod
    def validate_literal_value(cls, literal_value: Optional[Hashable]) -> None:
        assert isinstance(literal_value, uuid.UUID)


class BaseLiteralArray(BaseLiteral):
    """Base class for array literals"""

    __slots__ = ()


def _validate_array(literal_value: Optional[Hashable], item_type: type | tuple[type, ...]) -> None:
    assert isinstance(literal_value, tuple)
    for el in literal_value:
        assert el is None or isinstance(el, item_type)


class LiteralArrayInteger(BaseLiteralArray):
    """Represents an ARRAY_INT literal"""

    __slots__ = ()

    @classmethod
    def make(cls, value: list[int], *, meta: Optional[NodeMeta] = None) -> LiteralArrayInteger:
        return super().make(value=tuple(value), meta=meta)

    @classmethod
    def validate_literal_value(cls, literal_value: Optional[Hashable]) -> None:
        _validate_array(literal_value, int)


class LiteralArrayFloat(BaseLiteralArray):
    """Represents an ARRAY_FLOAT literal"""

    __slots__ = ()

    @classmethod
    def make(cls, value: list[float], *, meta: Optional[NodeMeta] = None) -> LiteralArrayFloat:
        return super().make(value=tuple(value), meta=meta)

    @classmethod
    def validate_literal_value(cls, literal_value: Optional[Hashable]) -> None:
        _validate_array(literal_value, float)


class LiteralArrayString(BaseLiteralArray):
    """Represents an ARRAY_STR literal"""

    __slots__ = ()

    @classmethod
    def make(cls, value: list[str], *, meta: Optional[NodeMeta] = None) -> LiteralArrayString:
        return super().make(value=tuple(value), meta=meta)

    @classmethod
    def validate_literal_value(cls, literal_value: Optional[Hashable]) -> None:
        _validate_array(literal_value, str)


class BaseLiteralTree(BaseLiteral):
    """Base class for tree literals"""

    __slots__ = ()


class LiteralTreeString(BaseLiteralArray):
    """Represents a TREE_STR literal"""

    __slots__ = ()

    @classmethod
    def make(cls, value: list[str], *, meta: Optional[NodeMeta] = None) -> LiteralTreeString:
        return super().make(value=tuple(value), meta=meta)

    @classmethod
    def validate_literal_value(cls, literal_value: Optional[Hashable]) -> None:
        _validate_array(literal_value, str)


class ExpressionList(FormulaItem):
    """A list of expressions (for use with the IN operator)"""

    __slots__ = ()
    autonomous = False

    @classmethod
    def make(cls: type[_FORMULA_ITEM_TV], *children: FormulaItem, meta: Optional[NodeMeta] = None) -> _FORMULA_ITEM_TV:
        return cls(*children, meta=meta)


_NAMED_ITEM_TV = TypeVar("_NAMED_ITEM_TV", bound="NamedItem")


class NamedItem(FormulaItem):
    __slots__ = ()
    show_names = FormulaItem.show_names + ("name",)

    @classmethod
    def validate_internal_value(cls, internal_value: tuple[Optional[Hashable], ...]) -> None:
        assert len(internal_value) == 1
        assert isinstance(internal_value[0], str)

    @property
    def name(self) -> str:
        return cast(str, self.internal_value[0])


class DimListNodeBase(FormulaItem):
    __slots__ = ()
    show_names = FormulaItem.show_names + ("dim_list",)

    dim_list: MultiChild[FormulaItem] = MultiChild(slice(0, None))  # TODO: rename to expr_list

    @classmethod
    def make(
        cls: type[_FORMULA_ITEM_TV],
        dim_list: Optional[Sequence[FormulaItem]],
        meta: Optional[NodeMeta] = None,
    ) -> _FORMULA_ITEM_TV:
        return cls(*(dim_list or ()), meta=meta)


class LodSpecifier(DimListNodeBase):
    __slots__ = ()
    autonomous = False


class InheritedLodSpecifier(LodSpecifier):
    """Represents LOD clause that inherits the original subquery's dimensions"""

    __slots__ = ()

    @classmethod
    def validate_children(cls, children: Sequence[FormulaItem]) -> None:
        assert not children, "Dimension list is not allowed for this type of LOD"


class FixedLodSpecifier(LodSpecifier):
    """Represents the ``FIXED <dimensions>`` LOD specifier clause"""

    __slots__ = ()


class IncludeLodSpecifier(LodSpecifier):
    """Represents the ``INCLUDE <dimensions>`` LOD specifier clause"""

    __slots__ = ()


class DefaultAggregationLodSpecifier(IncludeLodSpecifier):
    """
    Represents the default aggregation LOD specifier clause.
    Basically acts as an empty INCLUDE
    """

    __slots__ = ()


class ExcludeLodSpecifier(LodSpecifier):
    """Represents the ``EXCLUDE <dimensions>`` LOD specifier clause"""

    __slots__ = ()


_FUNC_CALL_TV = TypeVar("_FUNC_CALL_TV", bound="FuncCall")


class OperationCall(NamedItem):
    """Base class for functions and operators"""

    __slots__ = ()

    args: MultiChild[FormulaItem] = MultiChild(slice(0, None))


class FuncCall(OperationCall):
    """Generic function call"""

    __slots__ = ()
    show_names = OperationCall.show_names + ("args",)
    # Omit before_filter_by, ignore_dimensions and lod because they will be very rarely used explicitly

    args: MultiChild[FormulaItem] = MultiChild(slice(0, -3))
    lod: Child[LodSpecifier] = Child(-3)
    ignore_dimensions: Child[IgnoreDimensions] = Child(-2)
    before_filter_by: Child[BeforeFilterBy] = Child(-1)

    @classmethod
    def make(
        cls: type[_FUNC_CALL_TV],
        name: str,
        args: Iterable[FormulaItem],
        *,
        lod: Optional[LodSpecifier] = None,
        ignore_dimensions: Optional[IgnoreDimensions] = None,
        before_filter_by: Optional[BeforeFilterBy] = None,
        meta: Optional[NodeMeta] = None,
    ) -> _FUNC_CALL_TV:
        if lod is None:
            lod = DefaultAggregationLodSpecifier()
        if ignore_dimensions is None:
            ignore_dimensions = IgnoreDimensions()
        if before_filter_by is None:
            before_filter_by = BeforeFilterBy.make()

        children = (*args, lod, ignore_dimensions, before_filter_by)
        internal_value = (name,)
        return cls(*children, internal_value=internal_value, meta=meta)

    @classmethod
    def validate_children(cls, children: Sequence[FormulaItem]) -> None:
        assert len(children) >= 3
        assert isinstance(children[-3], LodSpecifier)
        assert isinstance(children[-2], IgnoreDimensions)
        assert isinstance(children[-1], BeforeFilterBy)


class Unary(OperationCall):
    """Unary (arithmetic or logical) operation"""

    __slots__ = ()
    show_names = OperationCall.show_names + ("expr",)

    expr: Child[FormulaItem] = Child(0)

    @classmethod
    def validate_children(cls, children: Sequence[FormulaItem]) -> None:
        assert len(children) == 1

    @classmethod
    def make(
        cls,
        name: str,
        expr: FormulaItem,
        *,
        meta: Optional[NodeMeta] = None,
    ) -> Unary:
        children = (expr,)
        internal_value = (name,)
        return cls(*children, internal_value=internal_value, meta=meta)


class Binary(OperationCall):
    """Binary (arithmetic or logical) operation"""

    __slots__ = ()
    show_names = OperationCall.show_names + ("left", "right")

    left: Child[FormulaItem] = Child(0)
    right: Child[FormulaItem] = Child(1)

    @classmethod
    def validate_children(cls, children: Sequence[FormulaItem]) -> None:
        assert len(children) == 2

    @classmethod
    def make(
        cls,
        name: str,
        left: FormulaItem,
        right: FormulaItem,
        *,
        meta: Optional[NodeMeta] = None,
    ) -> Binary:
        children = (left, right)
        internal_value = (name,)
        return cls(*children, internal_value=internal_value, meta=meta)


class Ternary(OperationCall):
    """Ternary (arithmetic or logical) operation"""

    __slots__ = ()
    show_names = OperationCall.show_names + ("first", "second", "third")

    first: Child[FormulaItem] = Child(0)
    second: Child[FormulaItem] = Child(1)
    third: Child[FormulaItem] = Child(2)

    @classmethod
    def validate_children(cls, children: Sequence[FormulaItem]) -> None:
        assert len(children) == 3

    @classmethod
    def make(
        cls,
        name: str,
        first: FormulaItem,
        second: FormulaItem,
        third: FormulaItem,
        *,
        meta: Optional[NodeMeta] = None,
    ) -> Ternary:
        children = (first, second, third)
        internal_value = (name,)
        return cls(*children, internal_value=internal_value, meta=meta)


class Field(NamedItem):
    """Database table field (column)"""

    __slots__ = ()

    @classmethod
    def make(cls, name: str, meta: Optional[NodeMeta] = None) -> Field:
        children = ()
        internal_value = (name,)
        return cls(*children, internal_value=internal_value, meta=meta)

    @classmethod
    def validate_children(cls, children: Sequence[FormulaItem]) -> None:
        assert not children


class WindowGrouping(DimListNodeBase):
    """Represents the grouping clause of window functions"""

    __slots__ = ()
    autonomous = False


class WindowGroupingTotal(WindowGrouping):
    """Represents the ``TOTAL`` grouping"""

    __slots__ = ()

    @classmethod
    def validate_children(cls, children: Sequence[FormulaItem]) -> None:
        assert not children, f"{cls.__name__} does not accept children"


class WindowGroupingWithin(WindowGrouping):
    """Represents the ``WITHIN <dimensions>`` grouping"""

    __slots__ = ()


class WindowGroupingAmong(WindowGrouping):
    """Represents the ``AMONG <dimensions>`` grouping"""

    __slots__ = ()


class Ordering(FormulaItem):
    """Represents an ORDER BY clause"""

    __slots__ = ()
    autonomous = False
    show_names = FormulaItem.show_names + ("expr_list",)

    expr_list: MultiChild[FormulaItem] = MultiChild(slice(0, None))

    @classmethod
    def make(
        cls,
        expr_list: Optional[list[FormulaItem]] = None,
        meta: Optional[NodeMeta] = None,
    ) -> Ordering:
        children = expr_list or ()
        internal_value = ()
        return cls(*children, internal_value=internal_value, meta=meta)


_FIELD_NAME_LIST_NODE_TV = TypeVar("_FIELD_NAME_LIST_NODE_TV", bound="FieldNameListNode")


class FieldNameListNode(FormulaItem):
    """Represents clause containing a field name list"""

    __slots__ = ()
    show_names = FormulaItem.show_names + ("field_names",)

    @classmethod
    def make(
        cls: type[_FIELD_NAME_LIST_NODE_TV],
        field_names: Optional[Collection[str]] = None,
        meta: Optional[NodeMeta] = None,
    ) -> _FIELD_NAME_LIST_NODE_TV:
        children = ()
        internal_value = (frozenset(field_names or ()),)
        return cls(*children, internal_value=internal_value, meta=meta)

    @classmethod
    def validate_children(cls, children: Sequence[FormulaItem]) -> None:
        assert not children

    @classmethod
    def validate_internal_value(cls, internal_value: tuple[Optional[Hashable], ...]) -> None:
        assert isinstance(internal_value, tuple)
        assert len(internal_value) == 1
        name_list = internal_value[0]
        assert isinstance(name_list, frozenset)
        for name in name_list:
            assert isinstance(name, str)

    @property
    def field_names(self) -> frozenset[str]:
        return cast(frozenset[str], self.internal_value[0])


class BeforeFilterBy(FieldNameListNode):
    """Represents a BEFORE FILTER BY clause"""

    __slots__ = ()
    autonomous = False


class IgnoreDimensions(DimListNodeBase):
    """Represents an IGNORE DIMENSIONS clause"""

    __slots__ = ()
    autonomous = False


class OrderingDirectionBase(ParenthesizedExpr):
    """Base class for ASC and DESC representations"""

    __slots__ = ()
    autonomous = False


class OrderAscending(OrderingDirectionBase):
    """Represents an ASC clause modifier"""

    __slots__ = ()


class OrderDescending(OrderingDirectionBase):
    """Represents an DESC clause modifier"""

    __slots__ = ()


_WINDOW_FUNC_CALL_TV = TypeVar("_WINDOW_FUNC_CALL_TV", bound="WindowFuncCall")


class WindowFuncCall(FuncCall):
    """Represents window function calls"""

    __slots__ = ()
    show_names = FuncCall.show_names + ("grouping", "ordering", "before_filter_by")

    args: MultiChild[FormulaItem] = MultiChild(slice(0, -5))
    ordering: Child[Ordering] = Child(-5)
    grouping: Child[WindowGrouping] = Child(-4)
    # lod is -3; ignore_dimensions  is -2; before_filter_by is -1.

    @classmethod
    def make(
        cls,
        name: str,
        args: Iterable[FormulaItem],
        *,
        ordering: Optional[Ordering] = None,
        grouping: Optional[WindowGrouping] = None,
        lod: Optional[LodSpecifier] = None,
        ignore_dimensions: Optional[IgnoreDimensions] = None,
        before_filter_by: Optional[BeforeFilterBy] = None,
        meta: Optional[NodeMeta] = None,
    ) -> WindowFuncCall:
        if grouping is None:
            grouping = WindowGroupingTotal()
        if ordering is None:
            ordering = Ordering()

        return super().make(
            name=name,
            args=[*args, ordering, grouping],
            lod=lod,
            before_filter_by=before_filter_by,
            ignore_dimensions=ignore_dimensions,
            meta=meta,
        )

    @classmethod
    def validate_children(cls, children: Sequence[FormulaItem]) -> None:
        super().validate_children(children=children)

        assert len(children) >= 5
        assert isinstance(children[-5], Ordering)
        assert isinstance(children[-4], WindowGrouping)


class IfPart(FormulaItem):
    __slots__ = ()
    autonomous = False
    show_names = FormulaItem.show_names + ("cond", "expr")

    cond: Child[FormulaItem] = Child(0)
    expr: Child[FormulaItem] = Child(1)

    @classmethod
    def validate_children(cls, children: Sequence[FormulaItem]) -> None:
        assert len(children) == 2

    @classmethod
    def validate_internal_value(cls, internal_value: tuple[Optional[Hashable], ...]) -> None:
        assert internal_value == ()

    @classmethod
    def make(
        cls,
        cond: FormulaItem,
        expr: FormulaItem,
        *,
        meta: Optional[NodeMeta] = None,
    ) -> IfPart:
        children = [cond, expr]
        return cls(*children, internal_value=(), meta=meta)


class IfBlock(FormulaItem):
    """
    Represents expressions like:
    IF condition_1 THEN expr_1 ELSEIF condition_2 THEN expr_2 ELSE expr_3 END
    """

    __slots__ = ()
    show_names = FormulaItem.show_names + ("if_list", "else_expr")

    if_list: MultiChild[IfPart] = MultiChild(slice(0, -1))
    else_expr: Child[FormulaItem] = Child(-1)

    @classmethod
    def validate_children(cls, children: Sequence[FormulaItem]) -> None:
        assert len(children) >= 2
        for child in children[:-1]:
            assert isinstance(child, IfPart)

    @classmethod
    def validate_internal_value(cls, internal_value: tuple[Optional[Hashable], ...]) -> None:
        assert internal_value == ()

    @classmethod
    def make(
        cls,
        if_list: list[IfPart],
        else_expr: Optional[FormulaItem] = None,
        *,
        meta: Optional[NodeMeta] = None,
    ) -> IfBlock:
        for part in if_list:
            check_type(part, IfPart)
        if else_expr is None:
            else_expr = Null()
        assert else_expr is not None
        children = [*if_list, else_expr]
        return cls(*children, internal_value=(), meta=meta)


class WhenPart(FormulaItem):
    __slots__ = ()
    autonomous = False
    show_names = FormulaItem.show_names + ("val", "expr")

    val: Child[FormulaItem] = Child(0)
    expr: Child[FormulaItem] = Child(1)

    @classmethod
    def validate_children(cls, children: Sequence[FormulaItem]) -> None:
        assert len(children) == 2

    @classmethod
    def validate_internal_value(cls, internal_value: tuple[Optional[Hashable], ...]) -> None:
        assert internal_value == ()

    @classmethod
    def make(
        cls,
        val: FormulaItem,
        expr: FormulaItem,
        meta: Optional[NodeMeta] = None,
    ) -> WhenPart:
        children = [val, expr]
        return cls(*children, meta=meta)


class CaseBlock(FormulaItem):
    """
    Represents expressions like:
    CASE expr WHEN value_1 THEN expr_1 WHEN value_2 THEN expr_2 ELSE expr_3 END
    """

    __slots__ = ()
    show_names = FormulaItem.show_names + ("case_expr", "when_list", "else_expr")

    case_expr: Child[FormulaItem] = Child(0)
    when_list: MultiChild[WhenPart] = MultiChild(slice(1, -1))
    else_expr: Child[FormulaItem] = Child(-1)

    @classmethod
    def validate_children(cls, children: Sequence[FormulaItem]) -> None:
        assert len(children) >= 2
        for child in children[1:-1]:
            assert isinstance(child, WhenPart)

    @classmethod
    def validate_internal_value(cls, internal_value: tuple[Optional[Hashable], ...]) -> None:
        assert internal_value == ()

    @classmethod
    def make(
        cls,
        case_expr: FormulaItem,
        when_list: list[WhenPart],
        else_expr: Optional[FormulaItem] = None,
        *,
        meta: Optional[NodeMeta] = None,
    ) -> CaseBlock:
        for part in when_list:
            check_type(part, WhenPart)
        if else_expr is None:
            else_expr = Null()
        assert else_expr is not None
        children = [case_expr, *when_list, else_expr]
        return cls(*children, meta=meta)


class Formula(ExprWrapper):
    """
    Top level wrapper for formula expressions.
    Provides some additional accessors for child nodes.
    """

    __slots__ = ()
