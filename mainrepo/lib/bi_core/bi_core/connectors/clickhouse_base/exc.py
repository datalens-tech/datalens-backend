from __future__ import annotations

import re
from typing import Any, Dict, Optional

from bi_core import exc


class ClickHouseSourceDoesNotExistError(exc.SourceDoesNotExist):
    ERR_RE = re.compile(r".*Table\s(?P<table>.*)\sdoesn't exist.*")

    def __init__(
        self,
        db_message: Optional[str] = None,
        query: Optional[str] = None,
        message: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        orig: Optional[Exception] = None,
        debug_info: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
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
