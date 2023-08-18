import sqlalchemy as sa
import sqlalchemy.dialects.oracle.base as sa_oracle
from sqlalchemy.types import TypeEngine

from bi_formula.core.datatype import DataType
from bi_formula.connectors.base.type_constructor import DefaultSATypeConstructor


class OracleTypeConstructor(DefaultSATypeConstructor):
    def get_sa_type(self, data_type: DataType) -> TypeEngine:
        type_map: dict[DataType, TypeEngine] = {
            DataType.BOOLEAN: sa_oracle.NUMBER(1, 0),
            DataType.DATETIME: sa.Date(),
            DataType.GENERICDATETIME: sa.Date(),
        }
        if (type_eng := type_map.get(data_type)) is not None:
            return type_eng
        else:
            return super().get_sa_type(data_type)
