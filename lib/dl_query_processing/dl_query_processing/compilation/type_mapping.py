from __future__ import annotations

from dl_constants.enums import UserDataType
from dl_formula.core.datatype import DataType


BI_TO_FORMULA_TYPES = {
    UserDataType.integer: DataType.INTEGER,
    UserDataType.float: DataType.FLOAT,
    UserDataType.boolean: DataType.BOOLEAN,
    UserDataType.string: DataType.STRING,
    UserDataType.date: DataType.DATE,
    UserDataType.datetime: DataType.DATETIME,
    UserDataType.datetimetz: DataType.DATETIMETZ,
    UserDataType.genericdatetime: DataType.GENERICDATETIME,
    UserDataType.geopoint: DataType.GEOPOINT,
    UserDataType.geopolygon: DataType.GEOPOLYGON,
    UserDataType.uuid: DataType.UUID,
    UserDataType.markup: DataType.MARKUP,
    UserDataType.unsupported: DataType.UNSUPPORTED,
    UserDataType.array_float: DataType.ARRAY_FLOAT,
    UserDataType.array_int: DataType.ARRAY_INT,
    UserDataType.array_str: DataType.ARRAY_STR,
    UserDataType.tree_str: DataType.TREE_STR,
}
FORMULA_TO_BI_TYPES = {
    **{ft: bit for bit, ft in BI_TO_FORMULA_TYPES.items()},
    **{ft.const_type: bit for bit, ft in BI_TO_FORMULA_TYPES.items()},
    DataType.NULL: UserDataType.string,  # NULL can in theory be any type, but we need to choose one
}
DEFAULT_DATA_TYPE = DataType.STRING
