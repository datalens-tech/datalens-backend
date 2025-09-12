import enum

import attr


@attr.s(frozen=True, kw_only=True)
class DataExportConnInfo:
    enabled_in_conn: bool = attr.ib(default=False)
    allowed_in_conn_type: bool = attr.ib(default=False)


@attr.s(frozen=True, kw_only=True)
class DataExportInfo(DataExportConnInfo):
    enabled_in_ds: bool = attr.ib(default=False)
    enabled_in_tenant: bool = attr.ib(default=False)
    background_allowed_in_tenant: bool = attr.ib(default=False)


class DataExportForbiddenReason(str, enum.Enum):
    disabled_in_ds = "DISABLED_EXPORT_DATASET"
    disabled_in_conn = "DISABLED_EXPORT_CONNECTION"
    disabled_in_tenant = "DISABLED_EXPORT_TENANT"
    prohibited_in_tenant = "PROHIBITED_EXPORT_TENANT"
    prohibited_in_conn_type = "PROHIBITED_EXPORT_CONNECTION"
    prohibited_in_pivot = "PROHIBITED_EXPORT_PIVOT_TABLE"
    prohibited_in_dashsql = "PROHIBITED_EXPORT_QL_CHART"
    prohibited_in_typed_query = "PROHIBITED_EXPORT_TYPED_QUERY"


@attr.s()
class DataExportInternalResult:
    allowed: bool = attr.ib(default=None)
    reason: list[str] = attr.ib(factory=list)


@attr.s()
class DataExportResult:
    basic: DataExportInternalResult = attr.ib()
    background: DataExportInternalResult = attr.ib()
