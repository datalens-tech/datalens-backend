from abc import ABC
import re
from typing import (
    Any,
    ClassVar,
    Optional,
)

from dl_core import exc


class TrinoSourceDoesNotExistError(exc.SourceDoesNotExist, ABC):
    ERR_RE: ClassVar[re.Pattern[str]]
    SOURCE_TYPE: ClassVar[str]

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
            if table := match.group(self.SOURCE_TYPE):
                self.params[f"{self.SOURCE_TYPE}_definition"] = table.strip("\"'")


class TrinoTableDoesNotExistError(TrinoSourceDoesNotExistError):
    SOURCE_TYPE = "table"
    ERR_RE = re.compile(r".*Table\s(?P<table>.*)\sdoes not exist.*")


class TrinoSchemaDoesNotExistError(TrinoSourceDoesNotExistError):
    SOURCE_TYPE = "schema"
    ERR_RE = re.compile(r".*Schema\s(?P<schema>.*)\sdoes not exist.*")


class TrinoCatalogDoesNotExistError(TrinoSourceDoesNotExistError):
    SOURCE_TYPE = "catalog"
    ERR_RE = re.compile(r".*Catalog\s(?P<catalog>.*)\snot found.*")
