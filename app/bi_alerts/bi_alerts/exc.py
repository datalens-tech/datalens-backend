from __future__ import annotations

from bi_constants.exc import DLBaseException


class DLAlertBaseError(DLBaseException):
    err_code = DLBaseException.err_code + ['ALERT']


class NotFoundError(DLAlertBaseError):
    err_code = DLAlertBaseError.err_code + ['NOT_FOUND']
    message = 'Object not found'
