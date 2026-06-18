from dl_constants.exc import DLBaseError


class CLSError(DLBaseError):
    err_code = (*DLBaseError.err_code, "CLS")
