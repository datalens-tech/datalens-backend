from __future__ import annotations

from typing import Optional, TypeVar

import attr

from bi_external_api import exc_defs
from bi_external_api.converter.workbook import WorkbookContext
from bi_external_api.domain import external as ext
from bi_external_api.exc_defs import ExternalAPIException
from bi_api_commons.exc import ExceptionWithData


@attr.s()
class WorkbookReadErrorInfo:
    wb_ctx: Optional[WorkbookContext] = attr.ib(default=None, repr=False)
    partial_workbook: Optional[ext.WorkBook] = attr.ib(default=None)
    conversion_errors: dict[str, list[Exception]] = attr.ib(factory=dict)
    unexpected_errors: list[Exception] = attr.ib(factory=list)


@attr.s()
class WorkbookClusterizationErrorInfo(WorkbookReadErrorInfo):
    # Workbook local names may be changed during clusterization.
    # So this section provides information to determine real ID/name/folder of entry.
    post_clusterization_name_map: Optional[list[ext.NameMapEntry]] = attr.ib(default=None)


_READ_LIKE_ERR_DATA_TV = TypeVar("_READ_LIKE_ERR_DATA_TV", bound=WorkbookReadErrorInfo)


class WorkbookReadBasePrivateError(ExternalAPIException, ExceptionWithData[_READ_LIKE_ERR_DATA_TV]):
    pass


class WorkbookReadPrivateError(WorkbookReadBasePrivateError[WorkbookReadErrorInfo]):
    """Indicates that workbook reading was finished with entry fetch or conversion errors"""
    pass


class WorkbookClustezationPrivateError(WorkbookReadBasePrivateError[WorkbookClusterizationErrorInfo]):
    """Indicates that workbook clusterization was finished with entry fetch or conversion errors"""
    pass


class GeneralConfigValidationException(exc_defs.ExternalAPIException):
    pass


@attr.s()
class OperationTerminationErrorData:
    user_message: str = attr.ib()


class OperationTerminationError(ExternalAPIException, ExceptionWithData[OperationTerminationErrorData]):
    """Indicates that execution of operation """
    pass
