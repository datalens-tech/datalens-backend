from __future__ import annotations

from bi_constants.enums import BIType

from bi_constants.types import BIDataTypes


DatalensDataTypes = BIDataTypes  # compatibility, to be deprecated  # FIXME


_BI_TO_YQL = {
    BIType.string: 'String',
    BIType.integer: 'Int64',
    BIType.float: 'Double',
    BIType.date: 'Date',
    BIType.datetime: 'Datetime',
    BIType.datetimetz: 'DatetimeTZ',
    BIType.genericdatetime: 'GenericDatetime',
    BIType.boolean: 'Bool',
    BIType.geopoint: 'GeoPoint',
    BIType.geopolygon: 'GeoPolygon',
    BIType.uuid: 'UUID',
    BIType.markup: 'Markup',
    BIType.array_str: 'ArrayStr',
    BIType.array_int: 'ArrayInt',
    BIType.array_float: 'ArrayFloat',
    BIType.tree_str: 'TreeStr',
    # Should not ever be in the output: `BIType.unsupported`
}


def bi_to_yql(bi_type: BIType) -> str:  # TODO: move to bi-api
    return _BI_TO_YQL[bi_type]
