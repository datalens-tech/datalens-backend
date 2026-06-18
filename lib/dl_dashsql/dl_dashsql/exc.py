from dl_constants.exc import DLBaseError


class DashSQLError(DLBaseError):
    err_code = (*DLBaseError.err_code, "DASHSQL")


class DashSQLParameterError(DashSQLError):
    err_code = (*DashSQLError.err_code, "PARAMETER")
