from __future__ import annotations

import re
from typing import (
    Any,
    Optional,
)

from dl_core import exc


class TrinoSourceDoesNotExistError(exc.SourceDoesNotExist):
    ERR_RE = re.compile(r".*Table\s(?P<table>.*)\sdoes not exist.*")

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
        super().__init__(
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
                self.params["table_definition"] = table.strip("\"'")
