from dl_constants.exc import DLBaseException


class CacheInvalidationBaseError(DLBaseException):
    err_code = DLBaseException.err_code + ["CACHE_INVALIDATION"]


class CacheInvalidationNonStringResultError(CacheInvalidationBaseError):
    err_code = CacheInvalidationBaseError.err_code + ["NON_STRING_RESULT"]
    default_message = "Cache invalidation query returned a non-string value, expected string"


class CacheInvalidationValueTooLongError(CacheInvalidationBaseError):
    err_code = CacheInvalidationBaseError.err_code + ["VALUE_TOO_LONG"]
    default_message = "Cache invalidation value exceeds the maximum allowed length"


class CacheInvalidationValidationError(CacheInvalidationBaseError):
    err_code = CacheInvalidationBaseError.err_code + ["VALIDATION_ERROR"]
    default_message = "Cache invalidation validation failed"


class CacheInvalidationDeserializationError(CacheInvalidationBaseError):
    err_code = CacheInvalidationBaseError.err_code + ["DESERIALIZATION_ERROR"]
    default_message = "Failed to deserialize cache invalidation entry"


class CacheInvalidationEmptyResultError(CacheInvalidationBaseError):
    err_code = CacheInvalidationBaseError.err_code + ["EMPTY_RESULT"]
    default_message = "Cache invalidation query returned no result"


class CacheInvalidationQueryTimeoutError(CacheInvalidationBaseError):
    err_code = CacheInvalidationBaseError.err_code + ["QUERY_TIMEOUT"]
    default_message = "Cache invalidation query timed out"


class CacheInvalidationQueryError(CacheInvalidationBaseError):
    err_code = CacheInvalidationBaseError.err_code + ["QUERY_ERROR"]
    default_message = "Cache invalidation query execution failed"


class CacheInvalidationSubselectNotAllowedError(CacheInvalidationBaseError):
    err_code = CacheInvalidationBaseError.err_code + ["SUBSELECT_NOT_ALLOWED"]
    default_message = "SQL mode cache invalidation requires subselect to be allowed on the connection"


class CacheInvalidationEmptySqlError(CacheInvalidationBaseError):
    err_code = CacheInvalidationBaseError.err_code + ["EMPTY_SQL"]
    default_message = "Cache invalidation SQL query is empty"


class CacheInvalidationMultipleRowsError(CacheInvalidationBaseError):
    err_code = CacheInvalidationBaseError.err_code + ["MULTIPLE_ROWS"]
    default_message = "Cache invalidation query returned multiple rows, expected exactly 1"


class CacheInvalidationEmptyRowError(CacheInvalidationBaseError):
    err_code = CacheInvalidationBaseError.err_code + ["EMPTY_ROW"]
    default_message = "Cache invalidation query returned a row with no columns"


class CacheInvalidationMultipleColumnsError(CacheInvalidationBaseError):
    err_code = CacheInvalidationBaseError.err_code + ["MULTIPLE_COLUMNS"]
    default_message = "Cache invalidation query returned multiple columns, expected exactly 1"
