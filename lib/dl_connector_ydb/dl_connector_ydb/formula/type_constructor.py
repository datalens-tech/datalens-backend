from sqlalchemy.types import TypeEngine

from dl_formula.connectors.base.type_constructor import DefaultSATypeConstructor
from dl_formula.core.datatype import DataType
import dl_sqlalchemy_ydb.dialect


class YDBTypeConstructor(DefaultSATypeConstructor):
    def get_sa_type(self, data_type: DataType) -> TypeEngine:
        type_map: dict[DataType, TypeEngine] = {
            DataType.DATETIME: dl_sqlalchemy_ydb.dialect.YqlDateTime(),
            DataType.GENERICDATETIME: dl_sqlalchemy_ydb.dialect.YqlDateTime(),
        }
        if (type_eng := type_map.get(data_type)) is not None:
            return type_eng
        else:
            return super().get_sa_type(data_type)
