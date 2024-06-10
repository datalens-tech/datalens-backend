from __future__ import annotations

from dl_constants.exc import DLBaseException


class FeatureNotAvailable(DLBaseException):
    err_code = DLBaseException.err_code + ["FEATURE_NOT_AVAILABLE"]


class RLSError(DLBaseException):
    err_code = DLBaseException.err_code + ["RLS"]


class RLSConfigParsingError(RLSError):
    err_code = RLSError.err_code + ["PARSE"]


class DatasetActionNotAllowedError(DLBaseException):
    err_code = DLBaseException.err_code + ["ACTION_NOT_ALLOWED"]


class UnsupportedForEntityType(DLBaseException):
    err_code = DLBaseException.err_code + ["UNSUPPORTED"]
    default_message = "This entity type does not support this operation"


class BadConnectionType(DLBaseException):
    err_code = DLBaseException.err_code + ["BAD_CONN_TYPE"]
    default_message = "Bad connection type's value"


class DatasetRevisionMismatch(DLBaseException):
    err_code = DLBaseException.err_code + ["DATASET_REVISION_MISMATCH"]
    default_message = "Dataset version mismatch. Refresh the page to continue."


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
