from __future__ import absolute_import

from dl_sqlalchemy_promql.connection import Connection
from dl_sqlalchemy_promql.errors import (
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
    "Connection",
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


def connect(*args, **kwargs):
    return Connection(*args, **kwargs)
