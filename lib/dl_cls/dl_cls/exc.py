from dl_constants.exc import DLBaseException


class CLSError(DLBaseException):
    err_code = (*DLBaseException.err_code, "CLS")
