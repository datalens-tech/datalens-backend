from __future__ import annotations

from typing import Optional, Tuple

import attr

from bi_constants.enums import QueryType, ConnectionType, NotificationLevel

from bi_api_commons.reporting import ReportingRecord


@attr.s(frozen=True, auto_attribs=True)
class QueryExecutionReportingRecord(ReportingRecord):
    query_id: str


@attr.s(frozen=True, auto_attribs=True)
class QueryExecutionStartReportingRecord(QueryExecutionReportingRecord):
    dataset_id: Optional[str]
    query_type: Optional[QueryType]
    connection_type: ConnectionType
    conn_reporting_data: dict
    query: str  # SQL query


@attr.s(frozen=True, auto_attribs=True)
class QueryExecutionCacheInfoReportingRecord(QueryExecutionReportingRecord):
    # Note: using it as ternary: hit / miss / undefined (error/disabled/...)
    # TODO?: write a bi_core.data_processing.cache_processing.CacheSituation name too?
    cache_full_hit: Optional[bool] = None


@attr.s(frozen=True, auto_attribs=True)
class QueryExecutionEndReportingRecord(QueryExecutionReportingRecord):
    exception: Optional[BaseException]


@attr.s(frozen=True, auto_attribs=True)
class DataProcessingReportingRecord(ReportingRecord):
    processing_id: str


@attr.s(frozen=True, auto_attribs=True)
class DataProcessingStartReportingRecord(DataProcessingReportingRecord):
    source_query_ids: Tuple[str, ...]


@attr.s(frozen=True, auto_attribs=True)
class DataProcessingEndReportingRecord(DataProcessingReportingRecord):
    exception: Optional[Exception]


@attr.s(frozen=True, auto_attribs=True)
class DataProcessingCacheInfoReportingRecord(DataProcessingReportingRecord):
    cache_full_hit: Optional[bool] = None


@attr.s(frozen=True, auto_attribs=True)
class NotificationReportingRecord(ReportingRecord):
    title: str
    message: str
    level: NotificationLevel
    locator: str
