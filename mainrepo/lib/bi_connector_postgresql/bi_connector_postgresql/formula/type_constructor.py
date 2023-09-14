import sqlalchemy.dialects.postgresql as sa_postgresql
from sqlalchemy.types import TypeEngine

from bi_formula.connectors.base.type_constructor import DefaultSATypeConstructor
from bi_formula.core.datatype import DataType


class PostgreSQLTypeConstructor(DefaultSATypeConstructor):
    def get_sa_type(self, data_type: DataType) -> TypeEngine:
        type_map: dict[DataType, TypeEngine] = {
            DataType.BOOLEAN: sa_postgresql.BOOLEAN(),
            DataType.UUID: sa_postgresql.UUID(),
        }
        if (type_eng := type_map.get(data_type)) is not None:
            return type_eng
        else:
            return super().get_sa_type(data_type)
