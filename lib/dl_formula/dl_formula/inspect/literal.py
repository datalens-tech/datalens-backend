from __future__ import annotations

from typing import Union

from dl_formula.core.datatype import DataType
import dl_formula.core.nodes as nodes


_TYPE_BY_CLS = {
    nodes.LiteralBoolean: DataType.CONST_BOOLEAN,
    nodes.LiteralDate: DataType.CONST_DATE,
    nodes.LiteralDatetime: DataType.CONST_DATETIME,
    nodes.LiteralDatetimeTZ: DataType.CONST_DATETIMETZ,
    nodes.LiteralGenericDatetime: DataType.CONST_GENERICDATETIME,
    nodes.LiteralFloat: DataType.CONST_FLOAT,
    nodes.LiteralGeopoint: DataType.CONST_GEOPOINT,
    nodes.LiteralGeopolygon: DataType.CONST_GEOPOLYGON,
    nodes.LiteralInteger: DataType.CONST_INTEGER,
    nodes.LiteralString: DataType.CONST_STRING,
    nodes.LiteralUuid: DataType.CONST_UUID,
    nodes.LiteralArrayInteger: DataType.CONST_ARRAY_INT,
    nodes.LiteralArrayFloat: DataType.CONST_ARRAY_FLOAT,
    nodes.LiteralArrayString: DataType.CONST_ARRAY_STR,
    nodes.LiteralTreeString: DataType.CONST_TREE_STR,
    nodes.Null: DataType.NULL,
}


def get_data_type(node: Union[nodes.BaseLiteral, nodes.Null]) -> DataType:
    if not isinstance(node, (nodes.BaseLiteral, nodes.Null)):
        raise TypeError(type(node))

    return _TYPE_BY_CLS[type(node)]
