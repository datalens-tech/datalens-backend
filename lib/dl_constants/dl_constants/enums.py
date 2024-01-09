"""
Common enums used in API and core internals of BI projects
"""

from __future__ import annotations

from enum import (
    Enum,
    auto,
    unique,
)
from typing import (
    Generic,
    Optional,
    Type,
    TypeVar,
    Union,
    cast,
)

from dynamic_enum import (
    AutoEnumValue,
    DynamicEnum,
)


_ENUM_TYPE = TypeVar("_ENUM_TYPE", bound=Enum)


class _Normalizable(Generic[_ENUM_TYPE]):
    @classmethod
    def normalize(cls, value: Union[_ENUM_TYPE, str, None]) -> Optional[_ENUM_TYPE]:
        if isinstance(value, str):
            value = cast(Type[_ENUM_TYPE], cls)[value]
        return value


@unique
class UserDataType(_Normalizable["UserDataType"], Enum):
    string = auto()
    integer = auto()
    float = auto()
    date = auto()
    datetime = auto()
    boolean = auto()
    geopoint = auto()
    geopolygon = auto()
    uuid = auto()
    markup = auto()
    datetimetz = auto()
    unsupported = auto()
    array_str = auto()
    array_int = auto()
    array_float = auto()
    tree_str = auto()
    genericdatetime = auto()


class SourceBackendType(DynamicEnum):
    NONE = AutoEnumValue()


class ConnectionType(DynamicEnum):
    unknown = AutoEnumValue()


@unique
class QueryBlockPlacementType(Enum):
    root = "root"
    after = "after"
    dispersed_after = "dispersed_after"


@unique
class FieldRole(Enum):
    row = "row"
    measure = "measure"  # TODO: Deprecate
    info = "info"
    order_by = "order_by"
    filter = "filter"
    parameter = "parameter"
    distinct = "distinct"
    range = "range"
    total = "total"
    template = "template"
    tree = "tree"


@unique
class FieldVisibility(Enum):
    visible = "visible"
    hidden = "hidden"


@unique
class PivotRole(Enum):
    pivot_row = "pivot_row"
    pivot_column = "pivot_column"
    pivot_measure = "pivot_measure"
    pivot_annotation = "pivot_annotation"
    pivot_info = "pivot_info"


@unique
class PivotHeaderRole(Enum):
    data = "data"
    total = "total"


@unique
class PivotItemType(Enum):
    stream_item = "stream_item"
    measure_name = "measure_name"
    dimension_name = "dimension_name"


@unique
class LegendItemType(Enum):
    field = "field"
    measure_name = "measure_name"
    dimension_name = "dimension_name"
    placeholder = "placeholder"


class QueryItemRefType(Enum):
    id = "id"
    title = "title"
    measure_name = "measure_name"
    dimension_name = "dimension_name"
    placeholder = "placeholder"


@unique
class ParameterValueConstraintType(Enum):
    all = "all"
    range = "range"
    set = "set"


@unique
class CalcMode(_Normalizable["CalcMode"], Enum):
    direct = "direct"
    formula = "formula"
    parameter = "parameter"


@unique
class ConditionPartCalcMode(Enum):
    direct = "direct"
    formula = "formula"
    result_field = "result_field"


@unique
class AggregationFunction(_Normalizable["AggregationFunction"], Enum):
    none = "none"
    sum = "sum"
    avg = "avg"
    min = "min"
    max = "max"
    count = "count"
    countunique = "countunique"
    # TODO: 'any'


@unique
class BinaryJoinOperator(Enum):
    gt = "gt"
    lt = "lt"
    gte = "gte"
    lte = "lte"
    eq = "eq"
    ne = "ne"


@unique
class FieldType(_Normalizable["FieldType"], Enum):
    DIMENSION = "DIMENSION"
    MEASURE = "MEASURE"


@unique
class WhereClauseOperation(Enum):
    # unary
    ISNULL = "isnull"
    ISNOTNULL = "isnotnull"

    # binary
    GT = "gt"
    LT = "lt"
    GTE = "gte"
    LTE = "lte"
    EQ = "eq"
    NE = "ne"
    STARTSWITH = "startswith"
    ISTARTSWITH = "istartswith"
    ENDSWITH = "endswith"
    IENDSWITH = "iendswith"
    CONTAINS = "contains"
    ICONTAINS = "icontains"
    NOTCONTAINS = "notcontains"
    NOTICONTAINS = "noticontains"
    LENEQ = "leneq"
    LENNE = "lenne"
    LENGT = "lengt"
    LENGTE = "lengte"
    LENLT = "lenlt"
    LENLTE = "lenlte"

    # binary with list
    IN = "in"
    NIN = "nin"

    # ternary
    BETWEEN = "between"


class DataSourceType(DynamicEnum):
    @classmethod
    def normalize(cls, value: DataSourceType | str | None) -> Optional[DataSourceType]:
        # FIXME: Remove this hack (used only in dsmaker)
        if isinstance(value, str):
            value = DataSourceType(value)
        return value


class DataSourceRole(Enum):
    origin = "origin"
    sample = "sample"
    materialization = "materialization"


class DataSourceCollectionType(Enum):
    collection = "collection"


class DataSourceCreatedVia(DynamicEnum):
    user = AutoEnumValue()


class RLSSubjectType(Enum):
    user = "user"  # `{value}: {login}` meaning `value allowed for user {login}`
    all = "all"  # `{value}: *` meaning 'value allowed for all subjects'
    userid = "userid"  # 'userid: userid' meaning 'filter by `field = {userid}`'
    unknown = "unknown"  # placeholder, should not be normally used.
    notfound = "notfound"  # subject that couldn't be found in subject resolver


class RLSPatternType(Enum):
    value = "user"
    all = "all"
    userid = "userid"


class ReportingQueryType(Enum):
    internal = "internal"
    external = "external"


class JoinConditionType(Enum):
    binary = "binary"


@unique
class JoinType(Enum):
    inner = "inner"
    left = "left"
    right = "right"
    full = "full"


@unique
class ManagedBy(_Normalizable["ManagedBy"], Enum):
    user = "user"
    feature = "feature"
    compiler_runtime = "compiler_runtime"


@unique
class USAuthMode(Enum):
    master = "master"
    public = "public"
    regular = "regular"


@unique
class ComponentType(Enum):
    data_source = "data_source"
    source_avatar = "source_avatar"
    avatar_relation = "avatar_relation"
    field = "field"
    obligatory_filter = "obligatory_filter"
    result_schema = "result_schema"


@unique
class TopLevelComponentId(Enum):
    result_schema = "__result_schema__"


@unique
class ComponentErrorLevel(Enum):
    error = "error"
    warning = "warning"


@unique
class OrderDirection(Enum):
    asc = "asc"
    desc = "desc"


@unique
class RangeType(Enum):
    min = "min"
    max = "max"


class ProcessorType(Enum):
    SOURCE_DB = auto()
    ASYNCPG = auto()
    AIOPG = auto()


@unique
class RawSQLLevel(Enum):
    off = "off"  # no raw sql allowed
    subselect = "subselect"  # wrapped raw SQL with `edit` permissions
    dashsql = "dashsql"  # unwrapped raw SQL with `execute` permissions


class DashSQLQueryType(DynamicEnum):
    classic_query = AutoEnumValue()
    # Connector-specific query types should be defined as "<connector_name>_<query_type>"


@unique
class NotificationStatus(Enum):
    OK = "OK"
    WARN = "WARN"
    CRIT = "CRIT"


class IndexKind(Enum):
    table_sorting = auto()


class RedisInstanceKind(Enum):
    caches = auto()
    persistent = auto()
    mutations = auto()
    arq = auto()


class FileProcessingStatus(Enum):
    in_progress = "in_progress"
    ready = "ready"
    failed = "failed"
    expired = "expired"


@unique
class NotificationLevel(Enum):
    info = "info"
    warning = "warning"
    critical = "critical"


class NotificationType(DynamicEnum):
    totals_removed_due_to_measure_filter = AutoEnumValue()


class ConnectorAvailability(Enum):
    free = "free"
    whitelist = "whitelist"


class QueryProcessingMode(DynamicEnum):
    basic = AutoEnumValue()
    no_compeng = AutoEnumValue()
    native_wf = AutoEnumValue()
