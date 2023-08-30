import sqlalchemy as sa
from sqlalchemy.sql.elements import ClauseElement

from bi_formula.core.datatype import DataType
from bi_formula.connectors.base.context_processor import BooleanlessContextPostprocessor


class MSSQLContextPostprocessor(BooleanlessContextPostprocessor):
    def booleanize_expression(self, data_type: DataType, expression: ClauseElement) -> ClauseElement:
        return sa.func.CONVERT(sa.text('BIT'), expression) == 1

    def debooleanize_expression(self, data_type: DataType, expression: ClauseElement) -> ClauseElement:
        return sa.func.IIF(expression, 1, 0)
