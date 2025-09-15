""" ... """

from __future__ import annotations

import cProfile
from contextlib import contextmanager
import datetime
import logging
import os
from typing import (
    TYPE_CHECKING,
    Any,
    Iterator,
    Optional,
)
import uuid

from dl_api_lib.common_models.data_export import (
    DataExportForbiddenReason,
    DataExportInfo,
    DataExportInternalResult,
    DataExportResult,
)
from dl_api_lib.enums import USPermissionKind
import dl_core.exc as common_exc


if TYPE_CHECKING:
    from dl_core.us_entry import USEntry

LOGGER = logging.getLogger(__name__)


@contextmanager
def query_execution_context(
    log_error: bool = True,
    dataset_id: Optional[str] = None,
    version: Optional[str] = None,
    body: Optional[dict] = None,
) -> Iterator[None]:
    try:
        yield  # execute query here
    except common_exc.DatabaseQueryError as err:
        if log_error:
            LOGGER.info(
                "Failed SQL query for dataset %s-%s. Request: %s.  Query: %s. Error: %s",
                dataset_id,
                version,
                str(body)[:1000],
                err.query,
                err.db_message,
                exc_info=True,
            )
        raise


@contextmanager
def profile_stats(stats_dir: Optional[str] = None) -> Iterator[None]:
    """Save profiler stats to file"""
    stats_dir = stats_dir or "./cprofiler"
    if not os.path.exists(stats_dir):
        os.makedirs(stats_dir)
    pr = cProfile.Profile()
    try:
        pr.enable()
        yield
    finally:
        pr.disable()
        filename = os.path.join(
            stats_dir,
            "{}-{}.stats".format(datetime.datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S"), str(uuid.uuid4())[:4]),
        )
        pr.dump_stats(filename)


def need_permission_on_entry(us_entry: USEntry, permission: USPermissionKind) -> None:
    assert us_entry.permissions is not None
    assert us_entry.uuid is not None
    if not us_entry.permissions[permission.name]:
        raise common_exc.USPermissionRequired(us_entry.uuid, permission.name)


def get_data_export_base_result(data_export_info: DataExportInfo) -> DataExportResult:
    is_basic_export_allowed = True
    is_background_export_allowed = True
    export_forbidden_reasons = []

    if not data_export_info.enabled_in_conn:
        is_basic_export_allowed = False
        is_background_export_allowed = False
        export_forbidden_reasons.append(DataExportForbiddenReason.disabled_in_conn.value)

    if not data_export_info.enabled_in_ds:
        is_basic_export_allowed = False
        is_background_export_allowed = False
        export_forbidden_reasons.append(DataExportForbiddenReason.disabled_in_ds.value)

    if not data_export_info.enabled_in_tenant:
        is_basic_export_allowed = False
        is_background_export_allowed = False
        export_forbidden_reasons.append(DataExportForbiddenReason.disabled_in_tenant.value)

    basic_export_forbidden_reasons = export_forbidden_reasons.copy()

    if not data_export_info.allowed_in_conn_type:
        is_background_export_allowed = False
        export_forbidden_reasons.append(DataExportForbiddenReason.prohibited_in_conn_type.value)

    if not data_export_info.background_allowed_in_tenant:
        is_background_export_allowed = False
        export_forbidden_reasons.append(DataExportForbiddenReason.prohibited_in_tenant.value)

    return DataExportResult(
        basic=DataExportInternalResult(
            allowed=is_basic_export_allowed,
            reason=basic_export_forbidden_reasons,
        ),
        background=DataExportInternalResult(
            allowed=is_background_export_allowed,
            reason=export_forbidden_reasons,
        ),
    )


def enrich_resp_dict_with_data_export_info(data: dict[str, Any], data_export_result: DataExportResult) -> None:
    is_basic_export_allowed = data_export_result.basic.allowed
    basic_export_forbidden_reason = None if is_basic_export_allowed else data_export_result.basic.reason

    is_background_export_allowed = data_export_result.background.allowed
    background_export_forbidden_reason = None if is_background_export_allowed else data_export_result.background.reason

    data["data_export"] = dict(
        basic=dict(
            allowed=is_basic_export_allowed,
            reason=basic_export_forbidden_reason,
        ),
        background=dict(
            allowed=is_background_export_allowed,
            reason=background_export_forbidden_reason,
        ),
    )
