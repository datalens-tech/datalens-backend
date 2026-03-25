from __future__ import annotations

from dl_constants.exc import DLBaseException


class FeatureNotAvailable(DLBaseException):
    err_code = DLBaseException.err_code + ["FEATURE_NOT_AVAILABLE"]


class DatasetActionNotAllowedError(DLBaseException):
    err_code = DLBaseException.err_code + ["ACTION_NOT_ALLOWED"]


class UnsupportedForEntityType(DLBaseException):
    err_code = DLBaseException.err_code + ["UNSUPPORTED"]
    default_message = "This entity type does not support this operation"
    forward_for_anonymous = True


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


class ExtractValidationError(DLValidationFatal):
    err_code = DLValidationError.err_code + ["EXTRACT"]


class ExtractFilterFieldMissing(ExtractValidationError):
    err_code = DLValidationError.err_code + ["FILTER_FIELD_MISSING"]


class ExtractSortingFieldMissing(ExtractValidationError):
    err_code = DLValidationError.err_code + ["SORTING_FIELD_MISSING"]


class ExtractSortingEmpty(ExtractValidationError):
    err_code = DLValidationError.err_code + ["SORTING_EMPTY"]


class CacheInvalidationTestError(DLBaseException):
    err_code = DLBaseException.err_code + ["CACHE_INVALIDATION_TEST"]
    default_message = "Cache invalidation test error"


class CacheInvalidationTestNotEditorError(CacheInvalidationTestError):
    err_code = CacheInvalidationTestError.err_code + ["NOT_EDITOR"]
    default_message = "Editor permissions are required to test cache invalidation"


class CacheInvalidationTestSubselectNotAllowedError(CacheInvalidationTestError):
    err_code = CacheInvalidationTestError.err_code + ["SUBSELECT_NOT_ALLOWED"]
    default_message = "Connection does not support subqueries required for SQL cache invalidation"


class CacheInvalidationTestInvalidResultError(CacheInvalidationTestError):
    err_code = CacheInvalidationTestError.err_code + ["INVALID_RESULT"]
    default_message = "Cache invalidation query returned an invalid result"


class CacheInvalidationTestModeOffError(CacheInvalidationTestError):
    err_code = CacheInvalidationTestError.err_code + ["MODE_OFF"]
    default_message = "Cache invalidation is not configured for this dataset"


class CacheInvalidationTestQueryError(CacheInvalidationTestError):
    err_code = CacheInvalidationTestError.err_code + ["QUERY_ERROR"]
    default_message = "Cache invalidation query execution failed"


class CacheInvalidationTestNonStringResultError(CacheInvalidationTestError):
    err_code = CacheInvalidationTestError.err_code + ["NON_STRING_RESULT"]
    default_message = "Cache invalidation query must return a string value"
