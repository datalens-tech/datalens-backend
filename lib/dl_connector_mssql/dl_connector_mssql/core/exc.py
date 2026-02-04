import re
from typing import (
    Any,
)

import dl_core.exc as exc


class SyncMssqlSourceDoesNotExistError(exc.SourceDoesNotExist):
    ERR_RE = re.compile(r".*Invalid\sobject\sname\s'(?P<table>.*)'\.\s\(208\).*")

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
        super(SyncMssqlSourceDoesNotExistError, self).__init__(
            db_message=db_message,
            query=query,
            inspector_query=inspector_query,
            message=message,
            details=details,
            orig=orig,
            debug_info=debug_info,
            params=params,
        )

        if self.orig and self.orig.args and len(self.orig.args) >= 2:
            message = self.orig.args[1]
            if message and (match := self.ERR_RE.match(message)):
                if table := match.group("table"):
                    self.params["table_definition"] = table


class CommitOrRollbackFailed(exc.DatabaseQueryError):
    err_code = exc.DatabaseQueryError.err_code + ["COMMIT_OR_ROLLBACK_FAILED"]
    default_message = "Failed to COMMIT or ROLLBACK"
