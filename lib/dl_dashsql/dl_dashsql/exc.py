from dl_constants.exc import DLBaseException


class DashSQLError(DLBaseException):
    err_code = DLBaseException.err_code + ["DASHSQL"]


class DashSQLParameterError(DashSQLError):
    err_code = DashSQLError.err_code + ["PARAMETER"]
