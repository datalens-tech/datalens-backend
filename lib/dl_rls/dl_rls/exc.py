from dl_constants.exc import DLBaseException


class RLSError(DLBaseException):
    err_code = DLBaseException.err_code + ["RLS"]


class RLSConfigParsingError(RLSError):
    err_code = RLSError.err_code + ["PARSE"]


class RLSSubjectNotFound(RLSError):
    err_code = DLBaseException.err_code + ["RLS_SUBJECT_NOT_FOUND"]
