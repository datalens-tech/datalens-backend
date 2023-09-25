"""
Only locally used non-API enums
"""

from __future__ import annotations

from enum import (
    Enum,
    unique,
)
from typing import Set

from dl_constants.enums import (
    BIType,
    WhereClauseOperation,
)
from dl_constants.enums import AggregationFunction as ag


W = WhereClauseOperation
_FILT_MINIMAL = frozenset((W.EQ, W.NE, W.IN, W.NIN))
_FILT_EQNEQ = frozenset((W.EQ, W.NE))
_FILT_COMP = _FILT_EQNEQ | frozenset((W.GT, W.GTE, W.LT, W.LTE))
_FILT_NULL = frozenset((W.ISNULL, W.ISNOTNULL))
_FILT_COMMON = _FILT_COMP | frozenset((W.IN, W.NIN, W.BETWEEN))
_FILT_STR = frozenset(
    (
        W.STARTSWITH,
        W.ISTARTSWITH,
        W.ENDSWITH,
        W.IENDSWITH,
        W.CONTAINS,
        W.ICONTAINS,
        W.NOTCONTAINS,
        W.NOTICONTAINS,
    )
)
_FILT_ARRAY = (
    _FILT_NULL
    | _FILT_EQNEQ
    | frozenset(
        (
            W.STARTSWITH,
            W.CONTAINS,
            W.NOTCONTAINS,
            W.LENEQ,
            W.LENNE,
            W.LENGT,
            W.LENGTE,
            W.LENLT,
            W.LENLTE,
        )
    )
)

FILTERS_BY_TYPE = {
    BIType.datetime: _FILT_NULL | _FILT_COMMON,
    BIType.datetimetz: _FILT_NULL | _FILT_COMMON,
    BIType.genericdatetime: _FILT_NULL | _FILT_COMMON,
    BIType.date: _FILT_NULL | _FILT_COMMON,
    BIType.boolean: _FILT_NULL | _FILT_MINIMAL,
    BIType.integer: _FILT_NULL | _FILT_COMMON | _FILT_STR,
    BIType.float: _FILT_NULL | _FILT_COMMON,
    BIType.string: _FILT_NULL | _FILT_COMMON | _FILT_STR,
    BIType.geopoint: _FILT_NULL | _FILT_MINIMAL,
    BIType.geopolygon: _FILT_NULL | _FILT_MINIMAL,
    BIType.uuid: _FILT_NULL | _FILT_COMMON | _FILT_STR,
    # intentionally not supporting comparison between markups:
    # it has too much dependency on internal optimizations.
    BIType.markup: _FILT_NULL,
    BIType.unsupported: _FILT_NULL,
    BIType.array_float: _FILT_ARRAY,
    BIType.array_int: _FILT_ARRAY,
    BIType.array_str: _FILT_ARRAY,
    BIType.tree_str: _FILT_ARRAY,
}

CASTS_BY_TYPE = {
    BIType.datetime: [
        BIType.genericdatetime,
        BIType.datetime,
        BIType.date,
        BIType.string,
        BIType.integer,
        BIType.float,
        BIType.boolean,
    ],
    BIType.datetimetz: [
        BIType.datetimetz,
        BIType.genericdatetime,
        BIType.date,
        BIType.string,
        BIType.integer,
        BIType.float,
        BIType.boolean,
    ],
    BIType.genericdatetime: [
        BIType.genericdatetime,
        BIType.date,
        BIType.string,
        BIType.integer,
        BIType.float,
        BIType.boolean,
    ],
    BIType.date: [BIType.date, BIType.genericdatetime, BIType.string, BIType.integer, BIType.float, BIType.boolean],
    BIType.boolean: [BIType.boolean, BIType.integer, BIType.float, BIType.string],
    BIType.integer: [BIType.integer, BIType.float, BIType.string, BIType.boolean, BIType.date, BIType.genericdatetime],
    BIType.float: [BIType.float, BIType.integer, BIType.string, BIType.boolean, BIType.date, BIType.genericdatetime],
    BIType.string: [
        BIType.string,
        BIType.date,
        BIType.genericdatetime,
        BIType.boolean,
        BIType.integer,
        BIType.float,
        BIType.geopoint,
        BIType.geopolygon,
    ],
    BIType.geopoint: [BIType.geopoint, BIType.string],
    BIType.geopolygon: [BIType.geopolygon, BIType.string],
    BIType.uuid: [BIType.uuid, BIType.string],
    BIType.markup: [BIType.markup],
    BIType.unsupported: [BIType.unsupported],  # `, BIType.string` would be too implicit.
    BIType.array_float: [BIType.array_float, BIType.string],
    BIType.array_int: [BIType.array_int, BIType.string],
    BIType.array_str: [BIType.array_str, BIType.string],
    BIType.tree_str: [BIType.tree_str],
}

_AGG_BASIC = [ag.none, ag.count]
BI_TYPE_AGGREGATIONS = {
    BIType.string: _AGG_BASIC + [ag.countunique],
    BIType.integer: _AGG_BASIC + [ag.sum, ag.avg, ag.min, ag.max, ag.countunique],
    BIType.float: _AGG_BASIC + [ag.sum, ag.avg, ag.min, ag.max, ag.countunique],
    BIType.date: _AGG_BASIC + [ag.min, ag.max, ag.countunique, ag.avg],
    BIType.datetime: _AGG_BASIC + [ag.min, ag.max, ag.countunique, ag.avg],
    BIType.datetimetz: _AGG_BASIC + [ag.min, ag.max, ag.countunique, ag.avg],
    BIType.genericdatetime: _AGG_BASIC + [ag.min, ag.max, ag.countunique],  # TODO: 'avg'?
    BIType.boolean: _AGG_BASIC + [],
    BIType.geopoint: _AGG_BASIC + [],
    BIType.geopolygon: _AGG_BASIC + [],
    BIType.uuid: _AGG_BASIC + [ag.countunique],
    BIType.markup: _AGG_BASIC + [],  # TODO: 'any'
    BIType.unsupported: [ag.none],  # only explicit formula-based processing is allowed
    BIType.array_float: _AGG_BASIC + [ag.countunique],
    BIType.array_int: _AGG_BASIC + [ag.countunique],
    BIType.array_str: _AGG_BASIC + [ag.countunique],
    BIType.tree_str: _AGG_BASIC + [ag.countunique],
}


@unique
class DatasetAction(Enum):
    # field
    update_field = "update_field"
    add_field = "add_field"
    delete_field = "delete_field"
    clone_field = "clone_field"

    # source
    update_source = "update_source"
    add_source = "add_source"
    delete_source = "delete_source"
    refresh_source = "refresh_source"

    # source avatar
    update_source_avatar = "update_source_avatar"
    add_source_avatar = "add_source_avatar"
    delete_source_avatar = "delete_source_avatar"

    # source relation
    update_avatar_relation = "update_avatar_relation"
    add_avatar_relation = "add_avatar_relation"
    delete_avatar_relation = "delete_avatar_relation"

    # connection actions
    replace_connection = "replace_connection"

    # filters
    update_obligatory_filter = "update_obligatory_filter"
    add_obligatory_filter = "add_obligatory_filter"
    delete_obligatory_filter = "delete_obligatory_filter"

    # TODO: remove legacy:
    update = "update"
    add = "add"
    delete = "delete"

    @staticmethod
    def remap_legacy(action: "DatasetAction") -> "DatasetAction":
        return _LEGACY_ACTIONS.get(action, action)

    @staticmethod
    def get_actions_whitelist_for_data_api() -> Set["DatasetAction"]:
        return {DatasetAction.add_field, DatasetAction.update_field, DatasetAction.delete_field}


_LEGACY_ACTIONS = {
    DatasetAction.add: DatasetAction.add_field,
    DatasetAction.update: DatasetAction.update_field,
    DatasetAction.delete: DatasetAction.delete_field,
}


@unique
class USPermissionKind(Enum):
    execute = "execute"
    read = "read"
    edit = "edit"
    admin = "admin"
