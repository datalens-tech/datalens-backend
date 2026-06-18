from typing import Any

# Standard exceptions according to dbapi v2 interface
# https://www.python.org/dev/peps/pep-0249/


class Error(Exception):
    pass


class Warning(Exception):  # noqa: N818
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


class MetrikaApiError(DatabaseError):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.orig_exc = kwargs.pop("orig_exc", None)
        super().__init__(*args, **kwargs)


class MetrikaHttpApiError(MetrikaApiError):
    pass


class MetrikaApiAccessDeniedError(MetrikaHttpApiError):
    pass


class MetrikaApiObjectNotFoundError(MetrikaHttpApiError):
    pass


class MetrikaApiGroupByNotSupportedError(NotSupportedError):
    pass


class MetrikaApiDimensionInCalcError(NotSupportedError):
    pass


class MetrikaApiNoMetricsNorGroupByError(ProgrammingError):
    pass


class ConnectionClosedError(MetrikaApiError):
    pass


class CursorClosedError(MetrikaApiError):
    pass
