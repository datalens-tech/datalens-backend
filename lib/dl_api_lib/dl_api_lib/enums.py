"""
Only locally used non-API enums
"""

from __future__ import annotations

from enum import (
    Enum,
    unique,
)

from dl_constants.enums import (
    UserDataType,
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
    UserDataType.datetime: _FILT_NULL | _FILT_COMMON,
    UserDataType.datetimetz: _FILT_NULL | _FILT_COMMON,
    UserDataType.genericdatetime: _FILT_NULL | _FILT_COMMON,
    UserDataType.date: _FILT_NULL | _FILT_COMMON,
    UserDataType.boolean: _FILT_NULL | _FILT_MINIMAL,
    UserDataType.integer: _FILT_NULL | _FILT_COMMON | _FILT_STR,
    UserDataType.float: _FILT_NULL | _FILT_COMMON,
    UserDataType.string: _FILT_NULL | _FILT_COMMON | _FILT_STR,
    UserDataType.geopoint: _FILT_NULL | _FILT_MINIMAL,
    UserDataType.geopolygon: _FILT_NULL | _FILT_MINIMAL,
    UserDataType.uuid: _FILT_NULL | _FILT_COMMON | _FILT_STR,
    # intentionally not supporting comparison between markups:
    # it has too much dependency on internal optimizations.
    UserDataType.markup: _FILT_NULL,
    UserDataType.unsupported: _FILT_NULL,
    UserDataType.array_float: _FILT_ARRAY,
    UserDataType.array_int: _FILT_ARRAY,
    UserDataType.array_str: _FILT_ARRAY,
    UserDataType.tree_str: _FILT_ARRAY,
}

CASTS_BY_TYPE = {
    UserDataType.datetime: [
        UserDataType.genericdatetime,
        UserDataType.datetime,
        UserDataType.date,
        UserDataType.string,
        UserDataType.integer,
        UserDataType.float,
        UserDataType.boolean,
    ],
    UserDataType.datetimetz: [
        UserDataType.datetimetz,
        UserDataType.genericdatetime,
        UserDataType.date,
        UserDataType.string,
        UserDataType.integer,
        UserDataType.float,
        UserDataType.boolean,
    ],
    UserDataType.genericdatetime: [
        UserDataType.genericdatetime,
        UserDataType.date,
        UserDataType.string,
        UserDataType.integer,
        UserDataType.float,
        UserDataType.boolean,
    ],
    UserDataType.date: [
        UserDataType.date,
        UserDataType.genericdatetime,
        UserDataType.string,
        UserDataType.integer,
        UserDataType.float,
        UserDataType.boolean,
    ],
    UserDataType.boolean: [UserDataType.boolean, UserDataType.integer, UserDataType.float, UserDataType.string],
    UserDataType.integer: [
        UserDataType.integer,
        UserDataType.float,
        UserDataType.string,
        UserDataType.boolean,
        UserDataType.date,
        UserDataType.genericdatetime,
    ],
    UserDataType.float: [
        UserDataType.float,
        UserDataType.integer,
        UserDataType.string,
        UserDataType.boolean,
        UserDataType.date,
        UserDataType.genericdatetime,
    ],
    UserDataType.string: [
        UserDataType.string,
        UserDataType.date,
        UserDataType.genericdatetime,
        UserDataType.boolean,
        UserDataType.integer,
        UserDataType.float,
        UserDataType.geopoint,
        UserDataType.geopolygon,
    ],
    UserDataType.geopoint: [UserDataType.geopoint, UserDataType.string],
    UserDataType.geopolygon: [UserDataType.geopolygon, UserDataType.string],
    UserDataType.uuid: [UserDataType.uuid, UserDataType.string],
    UserDataType.markup: [UserDataType.markup],
    UserDataType.unsupported: [UserDataType.unsupported],  # `, UserDataType.string` would be too implicit.
    UserDataType.array_float: [UserDataType.array_float, UserDataType.string],
    UserDataType.array_int: [UserDataType.array_int, UserDataType.string],
    UserDataType.array_str: [UserDataType.array_str, UserDataType.string],
    UserDataType.tree_str: [UserDataType.tree_str],
}

_AGG_BASIC = [ag.none, ag.count]
BI_TYPE_AGGREGATIONS = {
    UserDataType.string: _AGG_BASIC + [ag.countunique],
    UserDataType.integer: _AGG_BASIC + [ag.sum, ag.avg, ag.min, ag.max, ag.countunique],
    UserDataType.float: _AGG_BASIC + [ag.sum, ag.avg, ag.min, ag.max, ag.countunique],
    UserDataType.date: _AGG_BASIC + [ag.min, ag.max, ag.countunique, ag.avg],
    UserDataType.datetime: _AGG_BASIC + [ag.min, ag.max, ag.countunique, ag.avg],
    UserDataType.datetimetz: _AGG_BASIC + [ag.min, ag.max, ag.countunique, ag.avg],
    UserDataType.genericdatetime: _AGG_BASIC + [ag.min, ag.max, ag.countunique],  # TODO: 'avg'?
    UserDataType.boolean: _AGG_BASIC + [],
    UserDataType.geopoint: _AGG_BASIC + [],
    UserDataType.geopolygon: _AGG_BASIC + [],
    UserDataType.uuid: _AGG_BASIC + [ag.countunique],
    UserDataType.markup: _AGG_BASIC + [],  # TODO: 'any'
    UserDataType.unsupported: [ag.none],  # only explicit formula-based processing is allowed
    UserDataType.array_float: _AGG_BASIC + [ag.countunique],
    UserDataType.array_int: _AGG_BASIC + [ag.countunique],
    UserDataType.array_str: _AGG_BASIC + [ag.countunique],
    UserDataType.tree_str: _AGG_BASIC + [ag.countunique],
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

    # settings
    update_setting = "update_setting"

    # annotation
    update_annotation = "update_annotation"

    # TODO: remove legacy:
    update = "update"
    add = "add"
    delete = "delete"

    @staticmethod
    def remap_legacy(action: "DatasetAction") -> "DatasetAction":
        return _LEGACY_ACTIONS.get(action, action)

    @staticmethod
    def get_actions_whitelist_for_data_api() -> set["DatasetAction"]:
        return {DatasetAction.add_field, DatasetAction.update_field, DatasetAction.delete_field}


class DatasetSettingName(Enum):
    load_preview_by_default = "load_preview_by_default"
    template_enabled = "template_enabled"
    data_export_forbidden = "data_export_forbidden"


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
