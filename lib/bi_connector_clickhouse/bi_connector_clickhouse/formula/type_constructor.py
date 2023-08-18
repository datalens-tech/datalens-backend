import clickhouse_sqlalchemy.types as ch_types
from sqlalchemy.types import TypeEngine

from bi_formula.core.datatype import DataType
from bi_formula.connectors.base.type_constructor import DefaultSATypeConstructor


class ClickHouseTypeConstructor(DefaultSATypeConstructor):
    def get_sa_type(self, data_type: DataType) -> TypeEngine:
        type_map: dict[DataType, TypeEngine] = {
            **{
                str_type: ch_types.Nullable(ch_types.String)
                for str_type in (DataType.STRING, DataType.GEOPOINT, DataType.GEOPOLYGON,)
            },
            DataType.INTEGER: ch_types.Nullable(ch_types.Int64),
            DataType.FLOAT: ch_types.Nullable(ch_types.Float64),
            DataType.BOOLEAN: ch_types.Nullable(ch_types.UInt8),
            DataType.DATE: ch_types.Nullable(ch_types.Date),
            DataType.DATETIME: ch_types.Nullable(ch_types.DateTime),
            DataType.UUID: ch_types.Nullable(ch_types.UUID),
            DataType.ARRAY_INT: ch_types.Array(ch_types.Nullable(ch_types.Int64)),
            DataType.ARRAY_FLOAT: ch_types.Array(ch_types.Nullable(ch_types.Float64)),
            DataType.ARRAY_STR: ch_types.Array(ch_types.Nullable(ch_types.String)),
        }
        if (type_eng := type_map.get(data_type)) is not None:
            return type_eng
        else:
            return super().get_sa_type(data_type)
