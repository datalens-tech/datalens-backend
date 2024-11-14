from dl_constants.exc import DLBaseException


class DLS3Exception(DLBaseException):
    err_code = DLBaseException.err_code + ["FILE"]


class FileLimitError(DLS3Exception):
    err_code = DLS3Exception.err_code + ["FILE_LIMIT_EXCEEDED"]
    default_message = "The file is too large"
