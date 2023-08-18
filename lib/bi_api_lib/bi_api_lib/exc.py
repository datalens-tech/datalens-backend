from __future__ import annotations

from bi_constants.exc import (
    DLBaseException,
)


class FeatureNotAvailable(DLBaseException):
    err_code = DLBaseException.err_code + ['FEATURE_NOT_AVAILABLE']


class RLSError(DLBaseException):
    err_code = DLBaseException.err_code + ['RLS']


class RLSConfigParsingError(RLSError):
    err_code = RLSError.err_code + ['PARSE']


class IAMUserError(DLBaseException):
    pass


class BillingError(DLBaseException):
    err_code = DLBaseException.err_code + ['BILLING']


class DatasetActionNotAllowedError(DLBaseException):
    err_code = DLBaseException.err_code + ['ACTION_NOT_ALLOWED']


class BillingActionNotAllowed(BillingError):
    err_code = BillingError.err_code + ['ACTION_NOT_ALLOWED']


class BillingUnavailable(BillingError):
    err_code = BillingError.err_code + ['UNAVAILABLE']
    default_message = 'Billing service is not available'


class UnsupportedOptionError(DLBaseException):
    err_code = DLBaseException.err_code + ['UNSUPPORTED']
    default_message = 'Option is not supported'


class UnsupportedOffsetError(UnsupportedOptionError):
    err_code = UnsupportedOptionError.err_code + ['OFFSET']
    default_message = 'Offset is not supported'


class CompengError(DLBaseException):
    err_code = DLBaseException.err_code + ['COMPENG']
    default_message = 'Computational engine error'


class CompengNotSupported(CompengError):
    err_code = CompengError.err_code + ['UNSUPPORTED']
    default_message = 'Computational engine is not supported in synchronous API'


class UnsupportedForEntityType(DLBaseException):
    err_code = DLBaseException.err_code + ['UNSUPPORTED']
    default_message = 'This entity type does not support this operation'


class InvalidOrderBy(DLBaseException):
    err_code = DLBaseException.err_code + ['INVALID_ORDER_BY']
    default_message = 'Invalid result ordering configuration.'


class DatasetRevisionMismatch(DLBaseException):
    err_code = DLBaseException.err_code + ['DATASET_REVISION_MISMATCH']
    default_message = 'Dataset version mismatch. Refresh the page to continue.'


class _DLValidationResult(DLBaseException):
    err_code = DLBaseException.err_code + ['VALIDATION']
    default_message = ''


class DLValidationError(_DLValidationResult):
    err_code = _DLValidationResult.err_code + ['ERROR']
    default_message = 'Validation finished with errors.'


class TooManyFieldsError(DLValidationError):
    err_code = DLValidationError.err_code + ['TOO_MANY_FIELDS']


class DLValidationFatal(_DLValidationResult):
    err_code = _DLValidationResult.err_code + ['FATAL']
    default_message = 'Validation encountered a fatal error.'


class DashSQLError(DLBaseException):
    err_code = DLBaseException.err_code + ['DASHSQL']
