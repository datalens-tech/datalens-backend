"""
Non-"natural" nodes that can be used to facilitate translation, validation, slicing, etc.
"""

from __future__ import annotations

from typing import Hashable, Optional, Sequence, cast

import bi_formula.core.nodes as nodes


class ErrorNode(nodes.Null):
    """Node for setting up an error inside a formula to be raised by the translator"""

    __slots__ = ()
    show_names = nodes.FormulaItem.show_names + ('err_code', 'message')

    @property
    def err_code(self) -> tuple[str, ...]:
        return cast(tuple[str, ...], self.internal_value[0])

    @property
    def message(self) -> str:
        return cast(str, self.internal_value[1])

    @classmethod
    def make(
            cls, *,
            err_code: tuple[str, ...],
            message: str,
            meta: Optional[nodes.NodeMeta] = None,
    ) -> ErrorNode:
        children = ()
        internal_value = (err_code, message)
        return cls(*children, internal_value=internal_value, meta=meta)

    @classmethod
    def validate_children(cls, children: Sequence[nodes.FormulaItem]) -> None:
        assert not children

    @classmethod
    def validate_internal_value(cls, internal_value: tuple[Optional[Hashable], ...]) -> None:
        assert len(internal_value) == 2
        assert isinstance(internal_value[0], tuple)
        assert isinstance(internal_value[1], str)
