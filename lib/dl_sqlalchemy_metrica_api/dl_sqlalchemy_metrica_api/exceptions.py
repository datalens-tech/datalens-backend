# Standard exceptions according to dbapi v2 interface
# https://www.python.org/dev/peps/pep-0249/


class Error(Exception):
    pass


class Warning(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DataError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


# Dialect specific exceptions


class MetrikaApiException(DatabaseError):
    def __init__(self, *args, **kwargs):
        self.orig_exc = kwargs.pop("orig_exc", None)
        super().__init__(*args, **kwargs)


class MetrikaHttpApiException(MetrikaApiException):
    pass


class MetrikaApiAccessDeniedException(MetrikaHttpApiException):
    pass


class MetrikaApiObjectNotFoundException(MetrikaHttpApiException):
    pass


class MetrikaApiGroupByNotSupported(NotSupportedError):
    pass


class MetrikaApiDimensionInCalc(NotSupportedError):
    pass


class MetrikaApiNoMetricsNorGroupBy(ProgrammingError):
    pass


class ConnectionClosedException(MetrikaApiException):
    pass


class CursorClosedException(MetrikaApiException):
    pass
