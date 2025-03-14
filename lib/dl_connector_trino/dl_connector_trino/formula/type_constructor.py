from sqlalchemy.types import TypeEngine
from trino.sqlalchemy.datatype import parse_sqltype

from dl_formula.connectors.base.type_constructor import SATypeConstructor
from dl_formula.core.datatype import DataType


class TrinoTypeConstructor(SATypeConstructor):
    def get_sa_type(self, data_type: DataType) -> TypeEngine:
        return parse_sqltype(data_type)
