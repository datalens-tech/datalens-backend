from dl_constants.exc import DLBaseError


class RLSError(DLBaseError):
    err_code = (*DLBaseError.err_code, "RLS")


class RLSConfigParsingError(RLSError):
    err_code = (*RLSError.err_code, "PARSE")


class RLSSubjectNotFoundError(RLSError):
    err_code = (*DLBaseError.err_code, "RLS_SUBJECT_NOT_FOUND")
