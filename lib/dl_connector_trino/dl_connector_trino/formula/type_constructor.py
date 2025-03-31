import sqlalchemy as sa
from sqlalchemy.types import TypeEngine
import trino.sqlalchemy.datatype as tsa

from dl_formula.connectors.base.type_constructor import DefaultSATypeConstructor
from dl_formula.core.datatype import DataType


class TrinoTypeConstructor(DefaultSATypeConstructor):
    def get_sa_type(self, data_type: DataType) -> TypeEngine:
        type_map: dict[DataType, TypeEngine] = {
            **{
                str_type: sa.VARCHAR
                for str_type in (
                    DataType.STRING,
                    DataType.GEOPOINT,
                    DataType.GEOPOLYGON,
                    DataType.UUID,
                )
            },
            DataType.INTEGER: sa.BIGINT,
            DataType.FLOAT: tsa.DOUBLE,
            DataType.DATETIME: tsa.TIMESTAMP,
            DataType.ARRAY_INT: sa.ARRAY(sa.BIGINT),
            DataType.ARRAY_FLOAT: sa.ARRAY(tsa.DOUBLE),
            DataType.ARRAY_STR: sa.ARRAY(sa.VARCHAR),
        }
        return type_map.get(data_type, super().get_sa_type(data_type))
