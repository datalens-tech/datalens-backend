import re
from typing import Any

from dl_core import exc as exc


class PostgresSourceDoesNotExistError(exc.SourceDoesNotExist):
    ERR_RE = re.compile(r".*relation\s\"(?P<table>.*)\"\sdoes not exist.*")

    def __init__(
        self,
        db_message: str | None = None,
        query: str | None = None,
        inspector_query: str | None = None,
        message: str | None = None,
        details: dict[str, Any] | None = None,
        orig: Exception | None = None,
        debug_info: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ):
        super(PostgresSourceDoesNotExistError, self).__init__(
            db_message=db_message,
            query=query,
            inspector_query=inspector_query,
            message=message,
            details=details,
            orig=orig,
            debug_info=debug_info,
            params=params,
        )
        if self.db_message:
            message = self.db_message.replace("\n", "")
            if message and (match := self.ERR_RE.match(message)):
                if table := match.group("table"):
                    self.params["table_definition"] = table


class PgDoublePrecisionRoundError(exc.UnknownFunction):
    err_code = exc.DatabaseQueryError.err_code + ["PG_DOUBLE_PRECISION_ROUND"]
    default_message = (
        "ROUND with precision parameter is not supported for `double precision` data type " "in PostgreSQL data source."
    )
