from __future__ import annotations

import abc
import logging
import time
import traceback
from typing import Dict, Optional, Tuple, TYPE_CHECKING, Type, TypeVar, Union

import attr

from bi_constants.api_constants import DLContextKey

from bi_api_commons.base_models import RequestContextInfo
from bi_api_commons.reporting.models import (
    DataProcessingCacheInfoReportingRecord,
    DataProcessingEndReportingRecord,
    QueryExecutionCacheInfoReportingRecord,
    QueryExecutionEndReportingRecord,
    QueryExecutionReportingRecord,
    QueryExecutionStartReportingRecord,
    DataProcessingReportingRecord,
    DataProcessingStartReportingRecord,
)
from bi_api_commons.reporting.records import ReportingRecord

if TYPE_CHECKING:
    from bi_api_commons.reporting import ReportingRegistry


PROFILING_LOG_NAME = 'bi_core.profiling_db'
QUERY_PROFILING_ENTRY = 'dump request profile'


_RECORD_TV = TypeVar('_RECORD_TV', bound=ReportingRecord)


class ReportingProfiler(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def on_request_end(self) -> None:
        pass


@attr.s
class DefaultReportingProfiler(ReportingProfiler):
    _reporting_registry: ReportingRegistry = attr.ib(default=None)
    _is_public_env: bool = attr.ib(default=False)

    @property
    def reporting_registry(self) -> ReportingRegistry:
        return self._reporting_registry

    @property
    def rci(self) -> RequestContextInfo:
        return self._reporting_registry.rci

    def get_profiling_logger(self) -> logging.Logger:
        return logging.getLogger(PROFILING_LOG_NAME)

    def _find_record_of_type(
            self, records: Tuple[ReportingRecord, ...], rtype: Type[_RECORD_TV],
    ) -> Optional[_RECORD_TV]:
        return next(
            (r for r in records if isinstance(r, rtype)),
            None
        )

    def get_selector_records_for_query(self, query_id: str) -> Tuple[QueryExecutionReportingRecord, ...]:
        records = self.reporting_registry.get_reporting_records()
        return tuple(
            r for r in records
            if isinstance(r, QueryExecutionReportingRecord) and r.query_id == query_id
        )

    def get_data_proc_records_for_query(self, query_id: str) -> Tuple[DataProcessingReportingRecord, ...]:
        records = self.reporting_registry.get_reporting_records()
        proc_ids = {
            r.processing_id for r in records
            if isinstance(r, DataProcessingStartReportingRecord) and query_id in r.source_query_ids
        }
        return tuple(
            r for r in records
            if isinstance(r, DataProcessingReportingRecord) and r.processing_id in proc_ids
        )

    def flush_query_report(self, query_id: str) -> None:
        request_result_records = self.reporting_registry.get_request_result_reporting_records()
        query_records = self.get_selector_records_for_query(query_id=query_id)
        data_proc_records = self.get_data_proc_records_for_query(query_id=query_id)

        start_record = self._find_record_of_type(
            query_records, rtype=QueryExecutionStartReportingRecord)
        assert start_record is not None, 'need a matching `QueryExecutionStartReportingRecord`'
        end_selector_record = self._find_record_of_type(
            query_records, rtype=QueryExecutionEndReportingRecord)
        end_data_proc_record = self._find_record_of_type(
            query_records, rtype=DataProcessingEndReportingRecord)
        cache_selector_record = self._find_record_of_type(
            query_records, rtype=QueryExecutionCacheInfoReportingRecord)
        cache_data_proc_record = self._find_record_of_type(
            data_proc_records, rtype=DataProcessingCacheInfoReportingRecord)
        response_status_code, err_code = None, None
        if request_result_records:
            response_status_code = request_result_records[0].response_status_code
            err_code = request_result_records[0].err_code

        cache_used = cache_selector_record is not None or cache_data_proc_record is not None
        cache_full_hit = any(
            cache_record.cache_full_hit
            for cache_record in (cache_selector_record, cache_data_proc_record)
            if cache_record is not None
        )

        error: Optional[BaseException] = None
        end_record = end_data_proc_record if end_data_proc_record is not None else end_selector_record
        if end_record is not None:
            error = end_record.exception
            end_timestamp = end_record.timestamp
        else:
            end_timestamp = time.time()

        error_text: Optional[str]
        if error is None:
            error_text = None
        else:
            error_text = traceback.format_exception_only(type(error), error)[-1]

        x_dl_context = self.rci.x_dl_context
        extra: Dict[str, Union[str, int, float, None]] = dict(
            event_code='profile_db_request',
            # See:
            # https://a.yandex-team.ru/arc/trunk/arcadia/statbox/jam/libs/outer_action/datalens/bi_analytics/logs_config.py?rev=6459895#L64
            dataset_id=start_record.dataset_id,
            # auto: request_id
            user_id=self.rci.user_id,
            billing_folder_id=self.rci.get_tenant_id_if_cloud_env_none_else(),
            connection_type=start_record.connection_type.name,
            source=self.rci.endpoint_code,
            username=self.rci.user_name,
            execution_time=int(round((end_timestamp - start_record.timestamp) * 1000)),
            query=start_record.query,
            status='success' if error is None else 'error',
            error=error_text,
            err_code=err_code,
            cache_used=cache_used,
            cache_full_hit=cache_full_hit,

            # not in action fields:
            query_type=(
                start_record.query_type.name
                if start_record.query_type is not None
                else None),
            # TODO FIX: Use value from RCI when become done
            is_public=int(self._is_public_env),

            dash_id=x_dl_context.get(DLContextKey.DASH_ID),
            dash_tab_id=x_dl_context.get(DLContextKey.DASH_TAB_ID),
            chart_id=x_dl_context.get(DLContextKey.CHART_ID),
            chart_kind=x_dl_context.get(DLContextKey.CHART_KIND),

            response_status_code=response_status_code,

            **start_record.conn_reporting_data
        )
        # TODO FIX: Change logger
        log = self.get_profiling_logger()
        log.info(QUERY_PROFILING_ENTRY, extra=extra)

    def flush_all_query_reports(self) -> None:
        all_query_ids = sorted({
            r.query_id for r in self.reporting_registry.get_reporting_records()
            if isinstance(r, QueryExecutionReportingRecord)
        })
        for query_id in all_query_ids:
            self.flush_query_report(query_id=query_id)

    def on_request_end(self) -> None:
        self.flush_all_query_reports()
