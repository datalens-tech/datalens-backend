from __future__ import absolute_import

from bi_sqlalchemy_solomon.errors import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)

__all__ = (
    "connect",
    "DatabaseError",
    "DataError",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "Warning",
)


version = "0.0.1"

apilevel = "1.0"

threadsafety = 0

paramstyle = "named"


class Connection:
    def __init__(self, *args, **kwargs):
        pass

    def cursor(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


def connect(*args, **kwargs):
    return Connection(*args, **kwargs)
