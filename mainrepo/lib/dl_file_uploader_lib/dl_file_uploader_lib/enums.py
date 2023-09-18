from __future__ import annotations

from enum import Enum


class ErrorLevel(Enum):
    error = "error"
    warning = "warning"


class ErrorObjectKind(Enum):
    file = "file"
    source = "source"
    # system = 'system'


class FileType(Enum):
    csv = "csv"
    gsheets = "gsheets"
    xlsx = "xlsx"


class CSVEncoding(Enum):
    utf8 = "utf-8"
    windows1251 = "windows-1251"
    utf8sig = "utf-8-sig"
    utf16 = "utf-16"


class CSVDelimiter(Enum):
    comma = ","
    semicolon = ";"
    tab = "\t"


class RenameTenantStatus(Enum):
    scheduled = "scheduled"
    started = "started"
    error = "error"
    success = "success"
