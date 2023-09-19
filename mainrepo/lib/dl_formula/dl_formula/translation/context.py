from __future__ import annotations

import copy
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
)

from sqlalchemy.sql.elements import ClauseElement

from dl_formula.core.datatype import (
    DataType,
    DataTypeParams,
)
import dl_formula.core.exc as exc
from dl_formula.core.extract import NodeExtract
from dl_formula.core.message_ctx import (
    FormulaErrorCtx,
    MessageLevel,
)
from dl_formula.core.nodes import FormulaItem
from dl_formula.core.position import Position
from dl_formula.definitions.scope import Scope


ContextFlags = int  # bitwise combination of ContextFlag values


class TranslationCtx:
    __slots__ = (
        "collect_errors",
        "base_token",
        "children",
        "_messages",
        "forked",
        "data_type",
        "data_type_params",
        "expression",
        "flags",
        "node",
        "required_scopes",
    )
    _repr_attrs = (
        "base_token",
        "data_type",
        "data_type_params",
        "flags",
        "forked",
        "required_scopes",
        "node",
        "_expression_str",
        "children",
        "collect_errors",
        "_messages",
    )
    default_collect_errors = False

    def __init__(
        self,
        collect_errors: Optional[bool] = None,
        base_token: Optional[str] = None,
        flags: Optional[ContextFlags] = None,
        expression: Optional[ClauseElement] = None,
        data_type: Optional[DataType] = None,
        data_type_params: Optional[DataTypeParams] = None,
        node: Optional[FormulaItem] = None,
        required_scopes: int = Scope.EXPLICIT_USAGE,
    ):
        self.collect_errors = collect_errors if collect_errors is not None else self.default_collect_errors
        self.base_token = base_token
        self.children: List[TranslationCtx] = []
        self.forked = False  # TODO: think about removing this completely
        self.data_type = data_type
        self.data_type_params = data_type_params
        self.expression = expression
        self.flags = flags or 0
        self.node = node
        self.required_scopes: int = required_scopes

        self._messages: Dict[MessageLevel, List[FormulaErrorCtx]] = {
            MessageLevel.ERROR: [],
            MessageLevel.WARNING: [],
        }

    @property
    def _expression_str(self):
        try:
            return str(self.expression)
        except Exception:
            return repr(self.expression)

    def __repr__(self):
        _c0 = "\x1b[0m"
        _c1 = "\x1b[038;5;118m"
        return "<{}({})>{}".format(
            self.__class__.__name__,
            ",     ".join(f"{_c1}{key}{_c0}={getattr(self, key)!r}" for key in self._repr_attrs),
            _c0,
        )

    def copy(self) -> "TranslationCtx":
        return copy.copy(self)

    @property
    def extract(self) -> Optional[NodeExtract]:
        return self.node.extract if self.node is not None else None

    @property
    def errors(self) -> List[FormulaErrorCtx]:
        """Return list of registered errors."""
        return self._messages[MessageLevel.ERROR]

    @property
    def warnings(self) -> List[FormulaErrorCtx]:
        """Return list of registered warnings."""
        return self._messages[MessageLevel.WARNING]

    def flush(self) -> "TranslationCtx":
        """
        Flatten child contexts into self and validate:
        - aggregation consistency and
        - data type consistency (via type strategy)
        among children
        """

        for child in self.children:
            child.flush()

        for child in self.children:
            # propagate errors, warnings, etc.
            for key, messages in child._messages.items():
                self._messages[key].extend(messages)

        if not self.data_type:
            if self.forked:
                raise RuntimeError("Data type must be set for forked contexts")

            elif self.children:
                # propagate directly because there were no calculations here
                # (single child, no operator)
                self.data_type = self.children[0].data_type
                self.data_type_params = self.children[0].data_type_params
                self.node = self.children[0].node

            else:
                # some kind of exceptional case, no data type here, fall back to any type (NULL)
                self.data_type = DataType.NULL
                self.data_type_params = DataTypeParams()

        if not self.forked and self.children:  # single child, no operator
            if self.expression is None:
                # inherit the only child's expression
                self.expression = self.children[0].expression

            self.flags |= self.children[0].flags

        self.children.clear()
        self.forked = False
        return self

    def _add_message(
        self,
        level: MessageLevel,
        message: str,
        token: Optional[str] = None,
        code: Optional[Tuple[str, ...]] = None,
    ) -> None:
        token = token if token is not None else self.base_token
        position = self.node.position if self.node is not None else Position()
        error = exc.FormulaErrorCtx(
            message=message,
            level=level,
            token=token,
            position=position,
            code=tuple(code or ()),
        )

        if level is MessageLevel.ERROR and not self.collect_errors:
            raise exc.TranslationError(error)

        self._messages[level].append(error)

    def add_error(self, message: str, token: Optional[str] = None, code: Optional[Tuple[str, ...]] = None) -> None:
        return self._add_message(level=MessageLevel.ERROR, message=message, token=token, code=code)

    def add_warning(self, message: str, token: Optional[str] = None, code: Optional[Tuple[str, ...]] = None) -> None:
        return self._add_message(level=MessageLevel.WARNING, message=message, token=token, code=code)

    def set_type(self, data_type: DataType, data_type_params: Optional[DataTypeParams] = None) -> None:
        self.data_type = data_type
        self.data_type_params = data_type_params if data_type_params is not None else DataTypeParams()

    def set_expression(self, expression: Any) -> None:
        self.expression = expression

    def set_token(self, token: Optional[str]) -> None:
        self.base_token = token

    def set_flags(self, flags: ContextFlags) -> None:
        self.flags |= flags

    def fork(self) -> None:
        """Switch to muti-child mode. Should be called for function/operator nodes"""
        self.forked = True
        self.expression = None

    def _validate_can_add_new_child(self) -> None:
        if self.forked:
            pass
        else:
            if self.children:
                raise RuntimeError("Cannot have more than one child in non-forked mode")

    def child(
        self,
        token: Optional[str] = None,
        flags: Optional[int] = None,
        node: Optional[FormulaItem] = None,
        required_scopes: Optional[int] = None,
    ) -> "TranslationCtx":
        """
        Spawn new child context.
        In `forked` mode only one child is permitted.
        If multiple children are needed, i.e. this node is a function/operator,
        and children will be its arguments, then call `fork()` before spawning children.
        """

        self._validate_can_add_new_child()
        if not self.forked and node is not None:
            raise ValueError("Cannot assign node to child in non-forked mode")

        child = self.__class__(
            collect_errors=self.collect_errors,
            base_token=token if token is not None else self.base_token,
            flags=flags,
            node=node,
            required_scopes=self.required_scopes if required_scopes is None else required_scopes,
        )
        self.children.append(child)
        return child

    def adopt(self, child: "TranslationCtx") -> None:
        """
        "Adopt" a child context by adding it to children of self as if it were created by `self.fork()`.
        Can be used for substituting formula nodes for already translated expressions.
        """

        self._validate_can_add_new_child()
        self.children.append(child)
