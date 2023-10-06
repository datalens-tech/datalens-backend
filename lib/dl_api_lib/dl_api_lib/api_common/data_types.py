from __future__ import annotations

from dl_constants.enums import UserDataType


_BI_TO_YQL = {
    UserDataType.string: "String",
    UserDataType.integer: "Int64",
    UserDataType.float: "Double",
    UserDataType.date: "Date",
    UserDataType.datetime: "Datetime",
    UserDataType.datetimetz: "DatetimeTZ",
    UserDataType.genericdatetime: "GenericDatetime",
    UserDataType.boolean: "Bool",
    UserDataType.geopoint: "GeoPoint",
    UserDataType.geopolygon: "GeoPolygon",
    UserDataType.uuid: "UUID",
    UserDataType.markup: "Markup",
    UserDataType.array_str: "ArrayStr",
    UserDataType.array_int: "ArrayInt",
    UserDataType.array_float: "ArrayFloat",
    UserDataType.tree_str: "TreeStr",
    # Should not ever be in the output: `UserDataType.unsupported`
}


# TODO: Legacy stuff. Should be removed with data-api-v1
def bi_to_yql(bi_type: UserDataType) -> str:
    return _BI_TO_YQL[bi_type]
