from __future__ import annotations

from dataclasses import dataclass
from typing import (
    Any,
    ClassVar,
    Generic,
    Optional,
    TypeVar,
    Union,
)

from dl_formula.core.exc import ValidationError


_NODE_TV = TypeVar("_NODE_TV")
NodeActual = Union[_NODE_TV, str]  # what can act as a node
NodeInput = Union[_NODE_TV, str]  # what can be passed to node-builders


class MarkupProcessingBase(Generic[_NODE_TV]):
    quot: ClassVar[str] = '"'
    lpar: ClassVar[str] = "("
    rpar: ClassVar[str] = ")"
    sep: ClassVar[str] = " "

    node_text: ClassVar[str] = ""
    node_concat: ClassVar[str] = "c"
    node_url: ClassVar[str] = "a"
    node_i: ClassVar[str] = "i"
    node_b: ClassVar[str] = "b"
    node_sz: ClassVar[str] = "sz"
    node_cl: ClassVar[str] = "cl"
    node_br: ClassVar[str] = "br"
    node_userinfo: ClassVar[str] = "userinfo"
    node_image: ClassVar[str] = "img"
    node_tooltip: ClassVar[str] = "tooltip"

    _node_cls: ClassVar[type[_NODE_TV]]  # type: ignore  # 2024-01-24 # TODO: ClassVar cannot contain type variables  [misc]

    _dbg: bool = False

    empty_verbose_node = dict(type="concat", children=[])

    def _dbg_print(self, msg: str, *args: Any) -> None:
        if not self._dbg:
            return
        print("DBG:", msg % args)

    def _make_node(self, funcname: str, *funcargs: NodeActual) -> _NODE_TV:
        raise NotImplementedError

    def _unpack_node(self, node: _NODE_TV) -> tuple[str, tuple[NodeActual, ...]]:
        raise NotImplementedError

    class DumpError(ValueError):
        """..."""

    class ParseError(ValueError):
        """..."""

        # effective args: message, data, pos, name_of_expected_stuff

    # Helper functions

    def _quote_i(self, value: str) -> str:
        # `"` -> `""`
        if not isinstance(value, str):
            raise ValueError("should only quote strings")
        return value.replace(self.quot, self.quot + self.quot)

    def _quote(self, value: str) -> str:
        """
        ...

        >>> MarkupProcessing()._quote('ab"cd')
        '"ab""cd"'
        >>> MarkupProcessing()._quote('"') == '"' * 4
        True
        """
        return self.quot + self._quote_i(value) + self.quot

    def _proc_arg(self, value: NodeInput) -> NodeActual:
        if isinstance(value, self._node_cls):
            return value
        if isinstance(value, str):
            # return self._make_node(self.node_text, value)
            return value  # more compact, more db-expr-friendly.
        raise Exception("Unexpected value", value)

    # Node-building

    def n_concat(self, *children: NodeInput) -> _NODE_TV:
        return self._make_node(self.node_concat, *(self._proc_arg(child) for child in children))

    def n_url(self, addr: str, title: Optional[NodeInput] = None) -> _NODE_TV:
        return self._make_node(self.node_url, str(addr), self._proc_arg(title or addr))

    def n_i(self, child: NodeInput) -> _NODE_TV:
        return self._make_node(self.node_i, self._proc_arg(child))

    def n_b(self, child: NodeInput) -> _NODE_TV:
        return self._make_node(self.node_b, self._proc_arg(child))

    def n_sz(self, child: NodeInput, size: str) -> _NODE_TV:
        return self._make_node(self.node_sz, self._proc_arg(child), str(size))

    def n_cl(self, child: NodeInput, color: str) -> _NODE_TV:
        return self._make_node(self.node_cl, self._proc_arg(child), str(color))

    def n_br(self) -> _NODE_TV:
        return self._make_node(self.node_br)

    def n_userinfo(self, user_id: str, user_info: str) -> _NODE_TV:
        return self._make_node(self.node_userinfo, str(user_id), str(user_info))

    def n_img(self, src: str, width: int, height: int, alt: str) -> _NODE_TV:
        return self._make_node(self.node_image, src, width, height, alt)

    def n_tooltip(self, text: NodeInput, tooltip: NodeInput, placement: Optional[str] = "") -> _NODE_TV:
        if placement:
            return self._make_node(self.node_tooltip, self._proc_arg(text), self._proc_arg(tooltip), str(placement))
        else:
            return self._make_node(self.node_tooltip, self._proc_arg(text), self._proc_arg(tooltip))

    # Node-structure into a string

    def _dump_i(self, node: NodeActual) -> str:
        if isinstance(node, str):
            return self._quote(node)
        if isinstance(node, int):
            return self._quote(str(node))
        if node is None:
            return '""'
        if not isinstance(node, self._node_cls):
            raise ValueError("Value should be a node or a str")
        funcname, funcargs = self._unpack_node(node)
        return (
            self.lpar
            + funcname
            + self.sep
            + self.sep.join(
                # recurse
                self._dump_i(arg)
                for arg in funcargs
            )
            + self.rpar
        )

    def dump(self, node: _NODE_TV) -> str:
        if not isinstance(node, self._node_cls):
            raise self.DumpError("Outer value should be a node", type(node))
        return self._dump_i(node)

    # string into a node-structure

    def _get_at(self, data: str, pos: int, size: int, require: bool = True) -> str:
        result = data[pos : pos + size]
        if require and not result:
            raise self.ParseError("Unexpected data end", data, pos)
        return result

    def _is_at(self, data: str, pos: int, substr: str) -> bool:
        return self._get_at(data, pos, len(substr)) == substr

    def _find_next(self, data: str, substr: str, start: int, end: Optional[int] = None) -> Optional[int]:
        try:
            return data.index(substr, start, end)
        except ValueError:
            return None

    def _parse_i(self, data: str, pos: int = 0) -> tuple[NodeActual, int]:
        if self._dbg:
            self._dbg_print("_parse_i(%r <-%r-> %r)", data[:pos], pos, data[pos:])

        if self._is_at(data, pos, self.lpar):  # ({funcname} {arg1} {arg2} ...)
            if self._dbg:
                self._dbg_print("lpar @ %r: %r", pos, data[pos:])
            pos += len(self.lpar)

            next_rpar = self._find_next(data, self.rpar, pos)
            if next_rpar is None:
                raise self.ParseError("Unexpected data end", data, pos, "rpar")

            next_sep = self._find_next(data, self.sep, pos, next_rpar)
            if next_sep is None:  # '({funcname})'
                # Could do `next_sep = next_rpar` and fall through the `while`
                return self._make_node(data[pos:next_rpar]), next_rpar + len(self.rpar)

            # else: '({funcname} ...)'
            result = [data[pos:next_sep]]
            pos = next_sep + len(self.sep)
            while not self._is_at(data, pos, self.rpar):
                last_pos = pos
                node, pos = self._parse_i(data, pos)
                assert pos > last_pos, "should advance"
                result.append(node)
                if self._is_at(data, pos, self.rpar):
                    break
                if not self._is_at(data, pos, self.sep):
                    raise ValueError("Unexpected non-separator", data, pos, "sep")
                pos += len(self.sep)

            assert self._is_at(data, pos, self.rpar), "should have ended up at rpar"
            return self._make_node(*result), pos + len(self.rpar)

        if self._is_at(data, pos, self.quot):
            if self._dbg:
                self._dbg_print("quot @ %r: %r", pos, data[pos:])

            pos += len(self.quot)
            result_pieces = []
            last_pos = pos
            while True:
                next_quot = self._find_next(data, self.quot, pos)
                if next_quot is None:
                    raise self.ParseError("Unexpected data end", data, pos, "quot")
                if self._dbg:
                    self._dbg_print("next_quot @ %r: %r", next_quot, data[next_quot:])

                # check for quot2-after-quot
                quot2 = self.quot  # simple doubling
                if self._dbg:
                    self._dbg_print(
                        "_parse_i string %r -%r-> %r <-%r- %r",
                        data[:pos],
                        pos,
                        data[pos:next_quot],
                        next_quot,
                        data[next_quot:],
                    )

                result_pieces.append(data[pos:next_quot])
                if self._is_at(data, next_quot + len(self.quot), quot2):
                    result_pieces.append(quot2)
                    pos = next_quot + len(self.quot) + len(quot2)
                else:  # something else after the next_quot
                    self._dbg_print("end quot @ %r", next_quot)
                    return "".join(result_pieces), next_quot + len(self.quot)

                assert pos > last_pos, "should advance"
                last_pos = pos

        raise self.ParseError("Unexpected data", data, pos)

    def parse(self, data: Optional[str]) -> _NODE_TV:
        if data is None:
            return None  # type: ignore  # TODO: fix
        if not data.startswith(self.lpar) or not data.endswith(self.rpar):
            raise self.ParseError("Malformed data", data)
        node, pos = self._parse_i(data, 0)
        if pos != len(data):
            raise self.ParseError("Extra data at the end", data, pos)
        assert isinstance(node, self._node_cls)
        return node

    _verbalized_info = {
        node_text: dict(name="text", arg="content"),
        node_concat: dict(name="concat", args="children"),
        node_url: dict(name="url", argnames=("url", "content")),
        node_i: dict(name="italics", arg="content"),
        node_b: dict(name="bold", arg="content"),
        node_sz: dict(name="size", argnames=("content", "size")),
        node_cl: dict(name="color", argnames=("content", "color")),
        node_br: dict(name="br"),
        node_userinfo: dict(name="user_info", argnames=("content", "user_info")),
        node_image: dict(name="image", argnames=("src", "width", "height", "alt")),
        node_tooltip: dict(name="tooltip", argnames=("text", "tooltip", "placement")),
    }

    def _argcount_mismatch(self, node, **kwargs):  # type: ignore  # TODO: fix
        # Would be nice to do `self.DumpError("Argcount mismatch", node)` here,
        # but NULL-related behavior makes it unfeasible.
        # Thus, return an empty node (equivalent to an empty string).
        return self.empty_verbose_node

    def _verbalize_i(self, node: NodeActual) -> Any:
        """A kind-of the other way from dump: more verbose serializable structure. Still a dump."""
        if isinstance(node, str):
            return dict(type="text", content=node)

        if not isinstance(node, self._node_cls):
            raise self.DumpError("Value should be a node", type(node))

        funcname, funcargs = self._unpack_node(node)

        if funcname == self.node_concat:
            return dict(type="concat", children=[self._verbalize_i(arg) for arg in funcargs])
        if funcname == self.node_url:
            if len(funcargs) != 2:
                return self._argcount_mismatch(node=node, funcname=funcname, funcargs=funcargs, expected_args=2)
            url, content = funcargs
            return dict(type="url", url=url, content=self._verbalize_i(content))
        if funcname == self.node_cl:
            if len(funcargs) != 2:
                return self._argcount_mismatch(node=node, funcname=funcname, funcargs=funcargs, expected_args=2)
            content, color = funcargs
            return dict(type="color", color=color, content=self._verbalize_i(content))
        if funcname == self.node_sz:
            if len(funcargs) != 2:
                return self._argcount_mismatch(node=node, funcname=funcname, funcargs=funcargs, expected_args=2)
            content, size = funcargs
            return dict(type="size", size=size, content=self._verbalize_i(content))
        if funcname == self.node_userinfo:
            if len(funcargs) != 2:
                return self._argcount_mismatch(node=node, funcname=funcname, funcargs=funcargs, expected_args=2)
            content, user_info = funcargs
            return dict(type="user_info", user_info=user_info, content=self._verbalize_i(content))
        if funcname == self.node_br:
            if len(funcargs) != 0:
                return self._argcount_mismatch(node=node, funcname=funcname, funcargs=funcargs, expected_args=2)
            return dict(type="br")
        if funcname in (self.node_i, self.node_b):
            if len(funcargs) != 1:
                return self._argcount_mismatch(node=node, funcname=funcname, funcargs=funcargs, expected_args=1)
            return dict(
                type={self.node_i: "italics", self.node_b: "bold"}[funcname], content=self._verbalize_i(funcargs[0])
            )
        if funcname == self.node_image:
            if len(funcargs) != 4:
                return self._argcount_mismatch(node=node, funcname=funcname, funcargs=funcargs, expected_args=4)
            src, width, height, alt = funcargs
            src = None if src == "" else src
            alt = None if alt == "" else alt
            if width != "":
                width = int(width)
                if width < 0:
                    raise ValidationError("Width can only be greater than 0")
            else:
                width = None

            if height != "":
                height = int(height)
                if height < 0:
                    raise ValidationError("Height can only be greater than 0")
            else:
                height = None
            return dict(type="img", src=src, width=width, height=height, alt=alt)
        if funcname == self.node_tooltip:
            if len(funcargs) == 3:
                text, tooltip, placement = funcargs
            elif len(funcargs) == 2:
                text, tooltip = funcargs
                placement = "auto"
            else:
                return self._argcount_mismatch(node=node, funcname=funcname, funcargs=funcargs, expected_args=2)
            if placement not in ["top", "right", "bottom", "left", "auto"]:
                raise ValidationError('Placement can only be "top", "right", "bottom" or "left"')

            return dict(
                type="tooltip", content=self._verbalize_i(text), tooltip=self._verbalize_i(tooltip), placement=placement
            )

        raise self.DumpError("Unknown func", node)

    def verbalize(self, node: Optional[_NODE_TV]) -> Any:
        """A kind-of the other way from dump: more verbose serializable structure. Still a dump."""
        if node is None:
            return None
        if not isinstance(node, self._node_cls):
            raise self.DumpError("Outer value should be a node", type(node))
        return self._verbalize_i(node)

    def parse_and_verbalize(self, data: str) -> Any:
        return self.verbalize(self.parse(data))


_TNode = tuple
_TNodeActual = Union[_TNode, str]


class MarkupProcessing(MarkupProcessingBase[_TNode]):
    _node_cls: ClassVar[type[_TNode]] = tuple  # Node  # for isinstance checks

    def _make_node(self, funcname: str, *funcargs: _TNodeActual) -> _TNode:
        # return self._node_cls(funcname, *funcargs)
        return self._node_cls((funcname,) + funcargs)

    def _unpack_node(self, node: _TNode) -> tuple[str, tuple[_TNodeActual, ...]]:
        return node[0], node[1:]


@dataclass
class DCNode:
    funcname: str
    funcargs: tuple[NodeActual, ...]


_DCNode = DCNode
_DCNodeActual = Union[_DCNode, str]


class MarkupProcessingDC(MarkupProcessingBase[_DCNode]):
    _node_cls: ClassVar[type[_DCNode]] = DCNode

    def _make_node(self, funcname: str, *funcargs: _DCNodeActual) -> _DCNode:
        return self._node_cls(funcname=funcname, funcargs=funcargs)

    def _unpack_node(self, node: _DCNode) -> tuple[str, tuple[_DCNodeActual, ...]]:
        return node.funcname, node.funcargs


MARKUP_PROCESSING = MarkupProcessing()


postprocess_markup = MARKUP_PROCESSING.parse_and_verbalize
