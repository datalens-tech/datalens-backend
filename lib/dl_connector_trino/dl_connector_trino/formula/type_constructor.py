from sqlalchemy.types import TypeEngine

from dl_formula.connectors.base.type_constructor import DefaultSATypeConstructor
from dl_formula.core.datatype import DataType


class TrinoTypeConstructor(DefaultSATypeConstructor):
    def get_sa_type(self, data_type: DataType) -> TypeEngine:
        sa_type = super().get_sa_type(data_type)
        return sa_type
