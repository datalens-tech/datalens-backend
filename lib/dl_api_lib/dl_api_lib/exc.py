from __future__ import annotations

from dl_constants.exc import DLBaseError


class FeatureNotAvailableError(DLBaseError):
    err_code = (*DLBaseError.err_code, "FEATURE_NOT_AVAILABLE")


class DatasetActionNotAllowedError(DLBaseError):
    err_code = (*DLBaseError.err_code, "ACTION_NOT_ALLOWED")


class UnsupportedForEntityTypeError(DLBaseError):
    err_code = (*DLBaseError.err_code, "UNSUPPORTED")
    default_message = "This entity type does not support this operation"
    forward_for_anonymous = True


class BadConnectionTypeError(DLBaseError):
    err_code = (*DLBaseError.err_code, "BAD_CONN_TYPE")
    default_message = "Invalid connection type value"


class DatasetRevisionMismatchError(DLBaseError):
    err_code = (*DLBaseError.err_code, "DATASET_REVISION_MISMATCH")
    default_message = "Dataset version mismatch. Refresh the page to continue."


class WorkbookExportError(DLBaseError):
    err_code = (*DLBaseError.err_code, "WB_EXPORT")
    default_message = "Error while performing workbook export."


class DatasetExportError(WorkbookExportError):
    err_code = (*WorkbookExportError.err_code, "DS")
    default_message = "Error while performing dataset export."


class ConnectionExportError(WorkbookExportError):
    err_code = (*WorkbookExportError.err_code, "CONN")
    default_message = "Error while performing connection export."


class WorkbookImportError(DLBaseError):
    err_code = (*DLBaseError.err_code, "WB_IMPORT")
    default_message = "Error while performing workbook import."


class DatasetImportError(WorkbookImportError):
    err_code = (*WorkbookImportError.err_code, "DS")
    default_message = "Error while performing dataset import."


class ConnectionImportError(WorkbookImportError):
    err_code = (*WorkbookImportError.err_code, "CONN")
    default_message = "Error while performing connection import."


class _DLValidationResultError(DLBaseError):
    err_code = (*DLBaseError.err_code, "VALIDATION")
    default_message = ""


class DLValidationError(_DLValidationResultError):
    err_code = (*_DLValidationResultError.err_code, "ERROR")
    default_message = "Validation finished with errors."


class TooManyFieldsError(DLValidationError):
    err_code = (*DLValidationError.err_code, "TOO_MANY_FIELDS")


class DLValidationFatalError(_DLValidationResultError):
    err_code = (*_DLValidationResultError.err_code, "FATAL")
    default_message = "Validation encountered a fatal error."


class ConnectorIconNotFoundError(DLBaseError):
    default_message = "Connector icon not found"
    err_code = ("ICON_NOT_FOUND",)


class ExtractValidationError(DLValidationError):
    err_code = (*DLValidationError.err_code, "EXTRACT")


class ExtractValidationFilterFieldMissingError(ExtractValidationError):
    err_code = (*ExtractValidationError.err_code, "FILTER_FIELD_MISSING")


class ExtractValidationSortingFieldMissingError(ExtractValidationError):
    err_code = (*ExtractValidationError.err_code, "SORTING_FIELD_MISSING")


class ExtractValidationSortingEmptyError(ExtractValidationError):
    err_code = (*ExtractValidationError.err_code, "SORTING_EMPTY")


class CacheInvalidationTestError(DLBaseError):
    err_code = (*DLBaseError.err_code, "CACHE_INVALIDATION_TEST")
    default_message = "Cache invalidation test error"


class CacheInvalidationTestNotEditorError(CacheInvalidationTestError):
    err_code = (*CacheInvalidationTestError.err_code, "NOT_EDITOR")
    default_message = "Editor permissions are required to test cache invalidation"


class CacheInvalidationTestSubselectNotAllowedError(CacheInvalidationTestError):
    err_code = (*CacheInvalidationTestError.err_code, "SUBSELECT_NOT_ALLOWED")
    default_message = "Connection does not support subqueries required for SQL cache invalidation"


class CacheInvalidationTestInvalidResultError(CacheInvalidationTestError):
    err_code = (*CacheInvalidationTestError.err_code, "INVALID_RESULT")
    default_message = "Cache invalidation query returned an invalid result"


class CacheInvalidationTestModeOffError(CacheInvalidationTestError):
    err_code = (*CacheInvalidationTestError.err_code, "MODE_OFF")
    default_message = "Cache invalidation is not configured for this dataset"


class CacheInvalidationTestQueryError(CacheInvalidationTestError):
    err_code = (*CacheInvalidationTestError.err_code, "QUERY_ERROR")
    default_message = "Cache invalidation query execution failed"


class CacheInvalidationTestNonStringResultError(CacheInvalidationTestError):
    err_code = (*CacheInvalidationTestError.err_code, "NON_STRING_RESULT")
    default_message = "Cache invalidation query must return a string value"


class CacheInvalidationTestConnectionViewRequiredError(CacheInvalidationTestError):
    err_code = (*CacheInvalidationTestError.err_code, "CONNECTION_VIEW_REQUIRED")
    default_message = "View permission on the connection is required to test modified SQL invalidation queries"


class CacheInvalidationLastResultError(DLBaseError):
    err_code = (*DLBaseError.err_code, "CACHE_INVALIDATION_LAST_RESULT")
    default_message = "Cache invalidation last result error"


class CacheInvalidationLastResultNotEditorError(CacheInvalidationLastResultError):
    err_code = (*CacheInvalidationLastResultError.err_code, "NOT_EDITOR")
    default_message = "Editor permissions are required to view cache invalidation last result"


class CacheInvalidationLastResultNoSourcesError(CacheInvalidationLastResultError):
    err_code = (*CacheInvalidationLastResultError.err_code, "NO_SOURCES")
    default_message = "Dataset has no data sources"


class CacheInvalidationLastResultNoConnectionError(CacheInvalidationLastResultError):
    err_code = (*CacheInvalidationLastResultError.err_code, "NO_CONNECTION")
    default_message = "Could not determine connection"


class CacheInvalidationLastResultNotEnabledError(CacheInvalidationLastResultError):
    err_code = (*CacheInvalidationLastResultError.err_code, "NOT_ENABLED")
    default_message = "Cache invalidation is not enabled on the connection"


class CacheInvalidationLastResultNotSupportedError(CacheInvalidationLastResultError):
    err_code = (*CacheInvalidationLastResultError.err_code, "NOT_SUPPORTED")
    default_message = "Connection does not support cache invalidation"


class CacheInvalidationLastResultDisabledError(CacheInvalidationLastResultError):
    err_code = (*CacheInvalidationLastResultError.err_code, "DISABLED")
    default_message = "Cache invalidation is disabled in connection"


class CacheInvalidationLastResultEngineUnavailableError(CacheInvalidationLastResultError):
    err_code = (*CacheInvalidationLastResultError.err_code, "ENGINE_UNAVAILABLE")
    default_message = "Invalidation cache engine is not available"


class CacheInvalidationLastResultNoResultError(CacheInvalidationLastResultError):
    err_code = (*CacheInvalidationLastResultError.err_code, "NO_RESULT")
    default_message = "No cached invalidation result found"


class ConnectionTemplateNotFoundError(DLBaseError):
    err_code = (*DLBaseError.err_code, "CONNECTION_TEMPLATE_NOT_FOUND")
    default_message = "Connection has no associated template"


class PreviewSourceModificationNotAllowedError(DLBaseError):
    err_code = (*DLBaseError.err_code, "PREVIEW_SOURCE_MODIFICATION_NOT_ALLOWED")
    default_message = "Modifying data source parameters in /preview requires view permission on the connection"
