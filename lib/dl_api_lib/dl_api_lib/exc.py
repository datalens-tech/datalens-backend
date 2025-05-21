from __future__ import annotations

from dl_constants.exc import DLBaseException


class FeatureNotAvailable(DLBaseException):
    err_code = DLBaseException.err_code + ["FEATURE_NOT_AVAILABLE"]


class DatasetActionNotAllowedError(DLBaseException):
    err_code = DLBaseException.err_code + ["ACTION_NOT_ALLOWED"]


class UnsupportedForEntityType(DLBaseException):
    err_code = DLBaseException.err_code + ["UNSUPPORTED"]
    default_message = "This entity type does not support this operation"


class BadConnectionType(DLBaseException):
    err_code = DLBaseException.err_code + ["BAD_CONN_TYPE"]
    default_message = "Invalid connection type value"


class DatasetRevisionMismatch(DLBaseException):
    err_code = DLBaseException.err_code + ["DATASET_REVISION_MISMATCH"]
    default_message = "Dataset version mismatch. Refresh the page to continue."


class WorkbookExportError(DLBaseException):
    err_code = DLBaseException.err_code + ["WB_EXPORT"]
    default_message = "Error while performing workbook export."


class DatasetExportError(WorkbookExportError):
    err_code = WorkbookExportError.err_code + ["DS"]
    default_message = "Error while performing dataset export."


class ConnectionExportError(WorkbookExportError):
    err_code = WorkbookExportError.err_code + ["CONN"]
    default_message = "Error while performing connection export."


class WorkbookImportError(DLBaseException):
    err_code = DLBaseException.err_code + ["WB_IMPORT"]
    default_message = "Error while performing workbook import."


class DatasetImportError(WorkbookImportError):
    err_code = WorkbookImportError.err_code + ["DS"]
    default_message = "Error while performing dataset import."


class ConnectionImportError(WorkbookImportError):
    err_code = WorkbookImportError.err_code + ["CONN"]
    default_message = "Error while performing connection import."


class _DLValidationResult(DLBaseException):
    err_code = DLBaseException.err_code + ["VALIDATION"]
    default_message = ""


class DLValidationError(_DLValidationResult):
    err_code = _DLValidationResult.err_code + ["ERROR"]
    default_message = "Validation finished with errors."


class TooManyFieldsError(DLValidationError):
    err_code = DLValidationError.err_code + ["TOO_MANY_FIELDS"]


class DLValidationFatal(_DLValidationResult):
    err_code = _DLValidationResult.err_code + ["FATAL"]
    default_message = "Validation encountered a fatal error."


class ConnectorIconNotFoundException(DLBaseException):
    default_message = "Connector icon not found"
    err_code = ["ICON_NOT_FOUND"]
