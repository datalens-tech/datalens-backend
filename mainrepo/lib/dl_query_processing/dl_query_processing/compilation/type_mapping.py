from __future__ import annotations

from dl_constants.enums import BIType
from dl_formula.core.datatype import DataType


BI_TO_FORMULA_TYPES = {
    BIType.integer: DataType.INTEGER,
    BIType.float: DataType.FLOAT,
    BIType.boolean: DataType.BOOLEAN,
    BIType.string: DataType.STRING,
    BIType.date: DataType.DATE,
    BIType.datetime: DataType.DATETIME,
    BIType.datetimetz: DataType.DATETIMETZ,
    BIType.genericdatetime: DataType.GENERICDATETIME,
    BIType.geopoint: DataType.GEOPOINT,
    BIType.geopolygon: DataType.GEOPOLYGON,
    BIType.uuid: DataType.UUID,
    BIType.markup: DataType.MARKUP,
    BIType.unsupported: DataType.UNSUPPORTED,
    BIType.array_float: DataType.ARRAY_FLOAT,
    BIType.array_int: DataType.ARRAY_INT,
    BIType.array_str: DataType.ARRAY_STR,
    BIType.tree_str: DataType.TREE_STR,
}
FORMULA_TO_BI_TYPES = {
    **{ft: bit for bit, ft in BI_TO_FORMULA_TYPES.items()},
    **{ft.const_type: bit for bit, ft in BI_TO_FORMULA_TYPES.items()},
    DataType.NULL: BIType.string,  # NULL can in theory be any type, but we need to choose one
}
DEFAULT_DATA_TYPE = DataType.STRING
