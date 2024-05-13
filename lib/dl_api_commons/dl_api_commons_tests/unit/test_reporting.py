from __future__ import annotations

import uuid

import pytest

from dl_api_commons.base_models import (
    RequestContextInfo,
    TenantCommon,
)
from dl_api_commons.reporting.models import (
    QueryExecutionCacheInfoReportingRecord,
    QueryExecutionEndReportingRecord,
    QueryExecutionStartReportingRecord,
)
from dl_api_commons.reporting.profiler import DefaultReportingProfiler
from dl_api_commons.reporting.records import RequestResultReportingRecord
from dl_api_commons.reporting.registry import DefaultReportingRegistry
from dl_constants.api_constants import DLContextKey
from dl_constants.enums import (
    ConnectionType,
    ReportingQueryType,
)


CONNECTION_TYPE_CHYT_TEST = ConnectionType.declare("chyt_test")
CONNECTION_TYPE_TEST = ConnectionType.declare("test")


_QID = "some_qid_1234"

_DEFAULT_START_RECORD_TS_0 = QueryExecutionStartReportingRecord(
    query_id=_QID,
    timestamp=0,
    dataset_id="ds_123",
    connection_type=CONNECTION_TYPE_TEST,
    conn_reporting_data={
        "connection_id": "conn_123",
        "host": "8.8.8.8",
    },
    query_type=ReportingQueryType.external,
    query="SELECT 1",
    workbook_id="wb_123",
)

_DEFAULT_REPORT_FIELDS_FROM_START = dict(
    dataset_id="ds_123",
    connection_id="conn_123",
    connection_type=CONNECTION_TYPE_TEST.name,
    host="8.8.8.8",
    query_type=ReportingQueryType.external.name,
    query="SELECT 1",
    workbook_id="wb_123",
)

_CHYT_START_RECORD_TS_0 = QueryExecutionStartReportingRecord(
    query_id=_QID,
    timestamp=0,
    dataset_id="ds_123",
    connection_type=CONNECTION_TYPE_CHYT_TEST,
    conn_reporting_data={
        "connection_id": "conn_123",
        "cluster": "my_cluster",
        "clique_alias": "*ch_my_clique",
    },
    query_type=ReportingQueryType.external,
    query="SELECT 1",
    workbook_id=None,
)

_CHYT_REPORT_FIELDS_FROM_START = dict(
    dataset_id="ds_123",
    connection_id="conn_123",
    connection_type=CONNECTION_TYPE_CHYT_TEST.name,
    cluster="my_cluster",
    clique_alias="*ch_my_clique",
    query_type=ReportingQueryType.external.name,
    query="SELECT 1",
    workbook_id=None,
)


@pytest.mark.parametrize(
    "rci",
    [
        RequestContextInfo.create_empty(),
        RequestContextInfo.create(
            endpoint_code="ololo",
            tenant=TenantCommon(),
            user_name="user_name_123",
            user_id="uid_113",
            x_dl_context={k.value: str(uuid.uuid4()) for k in DLContextKey},
            plain_headers=None,
            secret_headers=None,
            auth_data=None,
            request_id="req999",
            x_dl_debug_mode=False,
        ),
    ],
)
@pytest.mark.parametrize(
    "case_name, records_seq, expected_query_data",
    [
        (
            "OK_no_cache_used",
            (
                _DEFAULT_START_RECORD_TS_0,
                QueryExecutionEndReportingRecord(query_id=_QID, timestamp=1, exception=None),
                RequestResultReportingRecord(
                    timestamp=1.0,
                    response_status_code=200,
                ),
            ),
            dict(
                _DEFAULT_REPORT_FIELDS_FROM_START,
                cache_used=False,
                cache_full_hit=False,
                status="success",
                execution_time=1000,
                error=None,
                err_code=None,
                response_status_code=200,
            ),
        ),
        (
            "OK_cache_hit",
            (
                _DEFAULT_START_RECORD_TS_0,
                QueryExecutionCacheInfoReportingRecord(query_id=_QID, timestamp=0.5, cache_full_hit=True),
                QueryExecutionEndReportingRecord(query_id=_QID, timestamp=1, exception=None),
                RequestResultReportingRecord(timestamp=1.0, response_status_code=200),
            ),
            dict(
                _DEFAULT_REPORT_FIELDS_FROM_START,
                cache_used=True,
                cache_full_hit=True,
                status="success",
                execution_time=1000,
                error=None,
                err_code=None,
                response_status_code=200,
            ),
        ),
        (
            "OK_no_cache_hit",
            (
                _DEFAULT_START_RECORD_TS_0,
                QueryExecutionCacheInfoReportingRecord(query_id=_QID, timestamp=0.5, cache_full_hit=False),
                QueryExecutionEndReportingRecord(query_id=_QID, timestamp=1, exception=None),
                RequestResultReportingRecord(
                    timestamp=1.0,
                    response_status_code=200,
                ),
            ),
            dict(
                _DEFAULT_REPORT_FIELDS_FROM_START,
                cache_used=True,
                cache_full_hit=False,
                status="success",
                execution_time=1000,
                error=None,
                err_code=None,
                response_status_code=200,
            ),
        ),
        (
            "ERR_no_cache_used",
            (
                _DEFAULT_START_RECORD_TS_0,
                QueryExecutionEndReportingRecord(query_id=_QID, timestamp=1, exception=ValueError("val err")),
                RequestResultReportingRecord(
                    timestamp=1.0,
                    response_status_code=500,
                    err_code="ERR.VAL_ERR",
                ),
            ),
            dict(
                _DEFAULT_REPORT_FIELDS_FROM_START,
                cache_used=False,
                cache_full_hit=False,
                status="error",
                execution_time=1000,
                error="ValueError: val err\n",
                err_code="ERR.VAL_ERR",
                response_status_code=500,
            ),
        ),
        (
            "OK_chyt",
            (
                _CHYT_START_RECORD_TS_0,
                QueryExecutionEndReportingRecord(query_id=_QID, timestamp=1, exception=None),
                RequestResultReportingRecord(
                    timestamp=1.0,
                    response_status_code=200,
                ),
            ),
            dict(
                _CHYT_REPORT_FIELDS_FROM_START,
                cache_used=False,
                cache_full_hit=False,
                status="success",
                execution_time=1000,
                error=None,
                err_code=None,
                response_status_code=200,
            ),
        ),
    ],
)
def test_db_query_report_generation(case_name, records_seq, expected_query_data, rci, caplog):
    required_extras = (
        "event_code",
        "dataset_id",
        "user_id",
        "connection_id",
        "connection_type",
        "source",
        "username",
        "execution_time",
        "query",
        "status",
        "error",
        "err_code",
        "cache_used",
        "cache_full_hit",
        "query_type",
        "is_public",
        "dash_id",
        "dash_tab_id",
        "chart_id",
        "chart_kind",
        "response_status_code",
        "workbook_id",
    )
    if "chyt" in case_name:
        required_extras += (
            "cluster",
            "clique_alias",
        )
    else:
        required_extras += ("host",)

    if rci.tenant is not None:
        required_extras += ("billing_folder_id",)

    expected_extras = dict(
        event_code="profile_db_request",
        is_public=0,
        dash_id=rci.x_dl_context.get(DLContextKey.DASH_ID),
        dash_tab_id=rci.x_dl_context.get(DLContextKey.DASH_TAB_ID),
        chart_id=rci.x_dl_context.get(DLContextKey.CHART_ID),
        chart_kind=rci.x_dl_context.get(DLContextKey.CHART_KIND),
        **expected_query_data,
        **rci.get_reporting_extra(),
    )

    assert set(expected_extras.keys()) == set(required_extras)

    rr = DefaultReportingRegistry(rci=rci)
    rep_profiler = DefaultReportingProfiler(reporting_registry=rr)
    for evt in records_seq:
        rr.save_reporting_record(evt)

    caplog.set_level("INFO")
    caplog.clear()

    rep_profiler.flush_query_report(next(iter(records_seq)).query_id)

    report_log_records = [r for r in caplog.records if getattr(r, "event_code", None) == "profile_db_request"]

    assert len(report_log_records) == 1, f"Got non-single query report in logs: {report_log_records}"
    log_record = report_log_records[0]

    actual_extras = {
        extra_name: getattr(log_record, extra_name) for extra_name in required_extras if hasattr(log_record, extra_name)
    }

    assert actual_extras == expected_extras
