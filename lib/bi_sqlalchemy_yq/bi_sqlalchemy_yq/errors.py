from __future__ import annotations

from ydb.dbapi.errors import _pretty_issues


class YQError(Exception):
    """..."""


class Warning(YQError):
    pass


class Error(YQError):
    def __init__(self, message, issues=None, status=None):
        try:
            pretty_issues = _pretty_issues(issues)
        except Exception:
            pretty_issues = None
        message = message if pretty_issues is None else pretty_issues

        super().__init__(message)
        self.issues = issues
        self.message = message
        self.status = status


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
