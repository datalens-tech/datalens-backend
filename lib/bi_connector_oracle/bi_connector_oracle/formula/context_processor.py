from typing import cast

import sqlalchemy as sa
from sqlalchemy.sql.elements import ClauseElement

from dl_formula.connectors.base.context_processor import BooleanlessContextPostprocessor
from dl_formula.core.datatype import DataType


class OracleContextPostprocessor(BooleanlessContextPostprocessor):
    def booleanize_expression(self, data_type: DataType, expression: ClauseElement) -> ClauseElement:
        if data_type in (
            DataType.CONST_BOOLEAN,
            DataType.CONST_INTEGER,
            DataType.CONST_FLOAT,
            DataType.BOOLEAN,
            DataType.INTEGER,
            DataType.FLOAT,
        ):
            expression = cast(ClauseElement, expression != 0)
        elif data_type in (DataType.CONST_STRING, DataType.STRING):
            expression = expression.isnot(None)
        elif data_type in (DataType.CONST_DATE, DataType.CONST_DATETIME, DataType.DATE, DataType.DATETIME):
            expression = cast(ClauseElement, sa.literal(1) == 1)
        else:
            expression = cast(ClauseElement, expression == 1)

        return expression

    def debooleanize_expression(self, data_type: DataType, expression: ClauseElement) -> ClauseElement:
        return sa.case(whens=[(expression, sa.literal(1))], else_=sa.literal(0))
