from __future__ import annotations

import enum
import logging
from typing import ClassVar, Optional, Union

import attr
from aiohttp import web


LOGGER = logging.getLogger(__name__)


class NumberFormatType(str, enum.Enum):
    """
    According to Google API Docs + several custom types
    https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellFormat
    """

    NUMBER_FORMAT_TYPE_UNSPECIFIED = 'NUMBER_FORMAT_TYPE_UNSPECIFIED'
    TEXT = 'TEXT'

    NUMBER = 'NUMBER'
    PERCENT = 'PERCENT'
    CURRENCY = 'CURRENCY'
    SCIENTIFIC = 'SCIENTIFIC'

    DATE = 'DATE'
    TIME = 'TIME'
    DATE_TIME = 'DATE_TIME'

    # not google types
    INTEGER = 'INTEGER'
    FLOAT = 'FLOAT'
    BOOLEAN = 'BOOLEAN'


@attr.s(auto_attribs=True)
class Cell:
    value: Optional[Union[str, int, float, bool]]
    number_format_type: NumberFormatType = NumberFormatType.NUMBER_FORMAT_TYPE_UNSPECIFIED
    empty: bool = False  # means this is an actually unfilled cell, not just clear


@attr.s(auto_attribs=True)
class Range:
    sheet_title: str

    col_from: int
    row_from: int
    col_to: int
    row_to: int

    def __str__(self) -> str:
        return '{sheet_title}!R{row_from}C{col_from}:R{row_to}C{col_to}'.format(**self.__dict__)


@attr.s(auto_attribs=True)
class Sheet:
    """
    id: number from /edit#gid=<sheetId>
    index: order of the sheet e.g. 0, 1, 2, 3, ...
    title: title of the sheet
    row_count: row count from sheet properties returned by sheets api
    column_count: column count from sheet properties returned by sheet api and reduced to cover data only in sample
    batch_size_rows: how many rows to request in `spreadsheets.get.values` in every batch to make responses around 4 MB
    data: actual grid data with values and formats
    """

    id: int
    index: int
    title: str
    row_count: int
    column_count: int
    batch_size_rows: int = 50
    data: Optional[list[list[Cell]]] = None

    def col_is_time(self, idx: int, has_header: bool) -> bool:
        """
        Check if column with given idx contains only TIME values (or TIME and nulls)
        """

        if not self.data or has_header and len(self.data) < 2:
            return False
        data_iter = iter(self.data)
        if has_header:
            next(data_iter)
        time_rows = 0
        for row in data_iter:
            if row[idx].number_format_type != NumberFormatType.TIME and not row[idx].empty:
                return False
            if row[idx].number_format_type == NumberFormatType.TIME:
                time_rows += 1
        return time_rows > 0


@attr.s(auto_attribs=True)
class Spreadsheet:
    id: str
    url: str  # TODO unused
    title: str
    sheets: list[Sheet]


@attr.s
class GSheetsSettings:
    APP_KEY: ClassVar[str] = 'GSHEETS_OAUTH2'

    api_key: str = attr.ib(repr=False)
    client_id: str = attr.ib(repr=False)
    client_secret: str = attr.ib(repr=False)

    def __attrs_post_init__(self):  # type: ignore  # monkeypatch point
        pass

    def bind_to_app(self, app: web.Application) -> None:
        app[self.APP_KEY] = self

    @classmethod
    def get_for_app(cls, app: web.Application) -> GSheetsSettings:
        return app[cls.APP_KEY]
