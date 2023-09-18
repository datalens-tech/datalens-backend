from __future__ import absolute_import

from bi_sqlalchemy_yq.connection import Connection
from bi_sqlalchemy_yq.errors import (
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

paramstyle = "qmark"


def connect(*args, **kwargs):
    return Connection(*args, **kwargs)
