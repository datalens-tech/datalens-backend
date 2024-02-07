from __future__ import annotations

import abc

from sqlalchemy.sql.elements import ClauseElement

from dl_formula.core.datatype import DataType
from dl_formula.definitions.flags import (
    ContextFlag,
    ContextFlags,
)


class ContextPostprocessor(abc.ABC):  # noqa: B024
    def get_warnings(self, data_type: DataType, flags: ContextFlags) -> list[str]:
        result: list[str] = []
        if flags & ContextFlag.DEPRECATED:
            result.append("Usage of deprecated function")

        return result

    def postprocess_expression(
        self,
        data_type: DataType,
        expression: ClauseElement,
        flags: ContextFlags,
    ) -> tuple[ClauseElement, ContextFlags]:
        return expression, flags


class BooleanlessContextPostprocessor(ContextPostprocessor):
    @abc.abstractmethod
    def booleanize_expression(self, data_type: DataType, expression: ClauseElement) -> ClauseElement:
        raise NotImplementedError

    @abc.abstractmethod
    def debooleanize_expression(self, data_type: DataType, expression: ClauseElement) -> ClauseElement:
        raise NotImplementedError

    def postprocess_expression(
        self,
        data_type: DataType,
        expression: ClauseElement,
        flags: ContextFlags,
    ) -> tuple[ClauseElement, ContextFlags]:
        if flags & ContextFlag.IS_CONDITION and not flags & ContextFlag.REQ_CONDITION:
            # expression is a boolean condition, which cannot be used as a value
            expression = self.debooleanize_expression(data_type=data_type, expression=expression)
            flags = flags | ContextFlag.REQ_CONDITION

        elif flags & ContextFlag.REQ_CONDITION and not flags & ContextFlag.IS_CONDITION:
            # function requires a boolean condition, which the given expression is not, so booleanize!
            expression = self.booleanize_expression(data_type=data_type, expression=expression)
            flags = flags | ContextFlag.IS_CONDITION

        return expression, flags
