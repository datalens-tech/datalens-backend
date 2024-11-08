from __future__ import annotations

import re
from typing import (
    Any,
    Optional,
)

from dl_core import exc


class ClickHouseSourceDoesNotExistError(exc.SourceDoesNotExist):
    ERR_RE = re.compile(r".*Table\s(?P<table>.*)\sdoesn't exist.*")

    def __init__(
        self,
        db_message: Optional[str] = None,
        query: Optional[str] = None,
        message: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
        orig: Optional[Exception] = None,
        debug_info: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
    ):
        super(ClickHouseSourceDoesNotExistError, self).__init__(
            db_message=db_message,
            query=query,
            message=message,
            details=details,
            orig=orig,
            debug_info=debug_info,
            params=params,
        )
        if self.db_message and (match := self.ERR_RE.match(self.db_message)):
            if table := match.group("table"):
                self.params["table_definition"] = table


class CannotInsertNullInOrdinaryColumn(exc.DatabaseQueryError):
    err_code = exc.DatabaseQueryError.err_code + ["CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN"]
    default_message = "Cannot convert NULL value to non-Nullable type."


class CHQueryError(exc.DatabaseQueryError):
    err_code = exc.DatabaseQueryError.err_code + ["CH"]
    default_message = "CH Error"


class CHIncorrectData(CHQueryError):
    err_code = CHQueryError.err_code + ["INCORRECT_DATA"]
    default_message = "Clickhouse could not parse the data in the specified source"


class CHReadonlyUser(CHQueryError):
    err_code = CHQueryError.err_code + ["READONLY_USER"]
    default_message = (
        "Clickhouse user must be correctly configured to use readonly 1 option (see docs). "
        "For other readonly options user should have parameter readonly set to 0 or 2."
    )


class EstimatedExecutionTooLong(exc.DatabaseQueryError):
    err_code = exc.DatabaseQueryError.err_code + ["EST_EXEC_TOO_LONG"]
    default_message = "Estimated query execution time is too long. Maximum: 14400."


class TooManyColumns(exc.DatabaseQueryError):
    err_code = exc.DatabaseQueryError.err_code + ["TOO_MANY_COLUMNS"]
    default_message = "Too many columns - limit exceeded."


class InvalidSplitSeparator(exc.DatabaseQueryError):
    err_code = exc.DatabaseQueryError.err_code + ["INVALID_SPLIT_SEPARATOR"]
    default_message = "Invalid separator for splitting."


class CHRowTooLarge(exc.SourceProtocolError):
    err_code = exc.SourceProtocolError.err_code + ["TOO_LARGE_ROW"]
    default_message = "Data source failed to respond correctly (too large row)."
