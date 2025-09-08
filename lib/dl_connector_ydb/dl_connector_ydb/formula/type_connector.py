import sqlalchemy as sa
from sqlalchemy.types import TypeEngine

from dl_formula.connectors.base.type_constructor import DefaultSATypeConstructor
from dl_formula.core.datatype import DataType


class YDBTypeConstructor(DefaultSATypeConstructor):
    def get_sa_type(self, data_type: DataType) -> TypeEngine:
        type_map: dict[DataType, TypeEngine] = {
            DataType.DATETIME: sa.DATETIME(),
            DataType.GENERICDATETIME: sa.DATETIME(),
        }
        if (type_eng := type_map.get(data_type)) is not None:
            return type_eng
        else:
            return super().get_sa_type(data_type)
