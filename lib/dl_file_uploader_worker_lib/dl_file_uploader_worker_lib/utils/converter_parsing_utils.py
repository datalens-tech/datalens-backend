from __future__ import annotations

import copy
import datetime
from functools import lru_cache
from itertools import islice
import logging
import re
import string
from typing import (
    Any,
    Callable,
    Iterator,
    Optional,
    Sequence,
    Union,
)
from uuid import uuid4

import attr

from dl_app_tools.profiling_base import (
    GenericProfiler,
    generic_profiler,
)
from dl_constants.enums import UserDataType
from dl_core import converter_types_cast
from dl_core.aio.web_app_services.gsheets import (
    Cell,
    NumberFormatType,
)
from dl_core.db import SchemaColumn

from dl_connector_bundle_chs3.chs3_base.core.type_transformer import DatetimeFileCommonTypeCaster


LOGGER = logging.getLogger(__name__)


class CsvParsingException(Exception):
    pass


DATE_RE = r"""
(?:
  [1-9]\d{3}-
  (?:
    (?:0[1-9]|1[0-2])
    -
    (?:0[1-9]|1\d|2[0-8])
    |
    (?:0[13-9]|1[0-2])
    -
    (?:29|30)
    |
    (?:0[13578]|1[02])
    -31
  )
|
  (?:
    [1-9]\d
    (?:0[48]|[2468][048]|[13579][26])
    |
    (?:[2468][048]|[13579][26])
    00
  )
  -02-29
)
"""


DATE_RU_FMT_RE = r"""
(?:
  (?:
    (?:0[1-9]|1\d|2[0-8])
    \.
    (?:0[1-9]|1[0-2])
    |
    (?:29|30)
    \.
    (?:0[13-9]|1[0-2])
    |
    31\.
    (?:0[13578]|1[02])
  )
  \.[1-9]\d{3}
|
  29\.02\.
  (?:
    [1-9]\d
    (?:0[48]|[2468][048]|[13579][26])
    |
    (?:[2468][048]|[13579][26])
    00
  )
)
"""


TIME_RE = r"""
(?:[01]\d|2[0-3])
:
[0-5]\d
:
[0-5]\d
(\.\d{1,6})?
"""


DATETIME_TZ_RE = r"""
(?:
  (Z|UTC)
|
  [+-][01]\d
  (:?[0-5]\d)?
)
"""


DATETIME_REGEXES = {
    "datetime__mid_space_without_tz": re.compile(rf"^{DATE_RE}\s{TIME_RE}$", flags=re.VERBOSE),
    "datetime__mid_space_with_tz": re.compile(rf"^{DATE_RE}\s{TIME_RE}\s?{DATETIME_TZ_RE}$", flags=re.VERBOSE),
    "datetime__full_with_tz": re.compile(rf"^{DATE_RE}T{TIME_RE}\s?{DATETIME_TZ_RE}$", flags=re.VERBOSE),
    "datetime__mid_t_without_tz": re.compile(rf"^{DATE_RE}T{TIME_RE}$", flags=re.VERBOSE),
    "datetime__ru_fmt_mid_space_without_tz": re.compile(rf"^{DATE_RU_FMT_RE}\s{TIME_RE}$", flags=re.VERBOSE),
}


@lru_cache(2**15)
def _check_datetime_re(value: str) -> None | str:
    if not value:
        return None
    for desc, re_expr in DATETIME_REGEXES.items():
        try:
            if re_expr.match(value):
                return desc
        except TypeError:
            return None
    return None


date_re = {
    "date__iso_date": re.compile(rf"^{DATE_RE}$", flags=re.VERBOSE),
    "date__reversed_full_year_slashes": re.compile(
        r"""
^
(
  (
    29
    /
    0?2
    /
    (?:
      [1-9]\d
      (?:0[48]|[2468][048]|[13579][26])
    |
      (?:[2468][048]|[13579][26])
      00
    )
  )
|
  (
    (?:0?[1-9]|1\d|2[0-8])
    /
    (?:0?[1-9]|1[0-2])
  |
    (?:29|30)
    /
    (?:0?[13-9]|1[0-2])
  |
    31
    /
    (?:0?[13578]|1[02])
  )
  /
  [1-9]\d{3}
)
$
""",
        flags=re.VERBOSE,
    ),
    "date__reversed_full_year_dots": re.compile(
        r"""
^
(
  (
    29
    \.
    0?2
    \.
    (?:
      [1-9]\d
      (?:0[48]|[2468][048]|[13579][26])
    |
      (?:[2468][048]|[13579][26])
      00
    )
  )
|
  (
    (?:0?[1-9]|1\d|2[0-8])
    \.
    (?:0?[1-9]|1[0-2])
  |
    (?:29|30)
    \.
    (?:0?[13-9]|1[0-2])
  |
    31
    \.
    (?:0?[13578]|1[02])
  )
  \.
  [1-9]\d{3}
)
$
""",
        flags=re.VERBOSE,
    ),
    "date__reversed_short_year_slashes": re.compile(
        r"""
^
(
  (
    29
    \.
    02
    \.
    (?:
      (?:00|0[48]|[2468][048]|[13579][26])
    )
  )
|
  (
    (?:0?[1-9]|1\d|2[0-8])
    \.
    (?:0?[1-9]|1[0-2])
  |
    (?:29|30)
    \.
    (?:0?[13-9]|1[0-2])
  |
    31
    \.
    (?:0?[13578]|1[02])
  )
  \.
  \d{2}
)
$
""",
        flags=re.VERBOSE,
    ),
    "date__reversed_short_year_dots": re.compile(
        r"""
^
(
  (
    29
    /
    02
    /
    (?:
      (?:00|0[48]|[2468][048]|[13579][26])
    )
  )
|
  (
    (?:0?[1-9]|1\d|2[0-8])
    /
    (?:0?[1-9]|1[0-2])
  |
    (?:29|30)
    /
    (?:0?[13-9]|1[0-2])
  |
    31
    /
    (?:0?[13578]|1[02])
  )
  /
  \d{2}
)
$
""",
        flags=re.VERBOSE,
    ),
}


@lru_cache(2**15)
def _check_date_re(value: str) -> None | str:
    if not value:
        return None
    for desc, re_expr in date_re.items():
        try:
            if re_expr.match(value):
                return desc
        except TypeError:
            return None
    return None


_TYPE_GROUP_NONE = 0
_TYPE_GROUP_BOOLEAN = 1
_TYPE_GROUP_NUMBER = 2
_TYPE_GROUP_DATETIME = 3
_TYPE_GROUP_STRING = 4


@attr.s(frozen=True)
class ParsingDataType:
    bi_type: UserDataType = attr.ib()
    type: Any = attr.ib()
    order: int = attr.ib()
    group: int = attr.ib()

    cast_func: Callable[[Any], Any] = attr.ib(default=None)
    check_func: Callable[[Any], Any] = attr.ib(default=None)
    format_desc: str = attr.ib(default=None)


_NONE_PARSING_DATA_TYPE = ParsingDataType(bi_type=UserDataType.string, type=str, order=-2, group=_TYPE_GROUP_NONE)
_BOOLEAN_PARSING_DATA_TYPE = ParsingDataType(
    bi_type=UserDataType.boolean,
    type=bool,
    order=-1,
    cast_func=converter_types_cast._to_boolean,
    group=_TYPE_GROUP_BOOLEAN,
)
_INTEGER_PARSING_DATA_TYPE = ParsingDataType(
    bi_type=UserDataType.integer, type=int, order=0, cast_func=converter_types_cast._to_int, group=_TYPE_GROUP_NUMBER
)
_FLOAT_PARSING_DATA_TYPE = ParsingDataType(
    bi_type=UserDataType.float, type=float, order=1, cast_func=converter_types_cast._to_float, group=_TYPE_GROUP_NUMBER
)
_DATE_PARSING_DATA_TYPE = ParsingDataType(
    bi_type=UserDataType.date,
    order=2,
    type=datetime.date,
    cast_func=converter_types_cast._to_date,
    check_func=_check_date_re,
    group=_TYPE_GROUP_DATETIME,
)
_DATETIME_PARSING_DATA_TYPE = ParsingDataType(
    bi_type=UserDataType.genericdatetime,
    order=3,
    type=datetime.datetime,
    cast_func=DatetimeFileCommonTypeCaster.cast_func,
    check_func=_check_datetime_re,
    group=_TYPE_GROUP_DATETIME,
)
_STRING_PARSING_DATA_TYPE = ParsingDataType(bi_type=UserDataType.string, type=str, order=4, group=_TYPE_GROUP_STRING)

ALLOWED_DATA_TYPES = (
    _INTEGER_PARSING_DATA_TYPE,
    _FLOAT_PARSING_DATA_TYPE,
    _DATE_PARSING_DATA_TYPE,
    _DATETIME_PARSING_DATA_TYPE,
    _STRING_PARSING_DATA_TYPE,
)
ALLOWED_DATA_TYPES_LEN = len(ALLOWED_DATA_TYPES)

TColumnTypes = dict[int, ParsingDataType]
TResultColumn = dict[str, Union[str, int]]
TResultTypes = list[TResultColumn]


@lru_cache(2**20)
def guess_cell_type(value: None | str, start_order: int = 0) -> ParsingDataType:
    if value is None or value == "":
        return _NONE_PARSING_DATA_TYPE

    result = None
    start_order = max(start_order, 0)
    for cell_type in islice(ALLOWED_DATA_TYPES, start_order, ALLOWED_DATA_TYPES_LEN):
        if cell_type.check_func is not None:
            format_desc = cell_type.check_func(value)
            if format_desc:
                return attr.evolve(cell_type, format_desc=format_desc)
        else:
            try:
                func = cell_type.cast_func or cell_type.type
                # call validation func. Exception ==> wrong type
                func(value)
            except (ValueError, TypeError, OverflowError):
                pass
            else:
                return cell_type

    if not result:
        raise CsvParsingException("Unknown data type")  # TODO: ignore unknown cell types


def guess_cell_type_gsheet(cell: Cell, *args: Any, **kwargs: Any) -> ParsingDataType:
    value = cell.value
    number_format = cell.number_format_type
    if value is None or value == "":
        return _NONE_PARSING_DATA_TYPE

    number_format_to_parsing_data_type_map = {
        NumberFormatType.NUMBER_FORMAT_TYPE_UNSPECIFIED: _STRING_PARSING_DATA_TYPE,
        NumberFormatType.TEXT: _STRING_PARSING_DATA_TYPE,
        NumberFormatType.TIME: _STRING_PARSING_DATA_TYPE,
        NumberFormatType.DATE: _DATE_PARSING_DATA_TYPE,
        NumberFormatType.DATE_TIME: _DATETIME_PARSING_DATA_TYPE,
        NumberFormatType.INTEGER: _INTEGER_PARSING_DATA_TYPE,
        NumberFormatType.FLOAT: _FLOAT_PARSING_DATA_TYPE,
        NumberFormatType.BOOLEAN: _BOOLEAN_PARSING_DATA_TYPE,
    }
    return number_format_to_parsing_data_type_map[number_format]


def _choose_new_cell_type(new_type: ParsingDataType, prev_type: ParsingDataType) -> Optional[ParsingDataType]:
    if prev_type.group == _TYPE_GROUP_NONE:
        return new_type
    if new_type.group == _TYPE_GROUP_NONE:
        return None
    if new_type.group == prev_type.group:
        if new_type.format_desc is not None and new_type.format_desc != prev_type.format_desc:
            # If date/datetime values have different formats, assume cell type is string
            return _STRING_PARSING_DATA_TYPE
        if new_type.order > prev_type.order:
            return new_type
        return None
    else:
        return _STRING_PARSING_DATA_TYPE


def generate_column_hash_name() -> str:
    return "f" + uuid4().hex[:16]


def guess_has_header(header_types: TColumnTypes, column_types: TColumnTypes) -> bool:
    has_header = False
    if len(header_types) >= len(column_types):
        header_type_gt_col = 0
        header_type_lt_col = 0
        for col_index, header_type in header_types.items():
            if len(column_types) <= col_index:
                continue

            if header_type.order < column_types[col_index].order:
                header_type_lt_col += 1
            elif header_type.order > column_types[col_index].order:
                header_type_gt_col += 1

        if header_type_lt_col == 0 and header_type_gt_col > 0:
            has_header = True
    return has_header


def merge_column_types(header_types: TColumnTypes, column_types: TColumnTypes, has_header: bool) -> TColumnTypes:
    if has_header:
        return column_types
    new_column_types = copy.deepcopy(column_types)
    for col_index, header_type in header_types.items():
        col_type = new_column_types[col_index]
        new_col_type = _choose_new_cell_type(new_type=header_type, prev_type=col_type)
        if new_col_type:
            new_column_types[col_index] = new_col_type
    return new_column_types


def raw_schema_to_column_types(raw_schema: list[SchemaColumn]) -> TColumnTypes:
    bi_type_to_parsing_data_type_map = {
        UserDataType.integer: _INTEGER_PARSING_DATA_TYPE,
        UserDataType.float: _FLOAT_PARSING_DATA_TYPE,
        UserDataType.date: _DATE_PARSING_DATA_TYPE,
        UserDataType.genericdatetime: _DATETIME_PARSING_DATA_TYPE,
        UserDataType.datetime: _DATETIME_PARSING_DATA_TYPE,
        UserDataType.string: _STRING_PARSING_DATA_TYPE,
        UserDataType.boolean: _BOOLEAN_PARSING_DATA_TYPE,
    }
    column_types = {}
    for col_index, col in enumerate(raw_schema):
        column_types[col_index] = bi_type_to_parsing_data_type_map.get(col.user_type, _STRING_PARSING_DATA_TYPE)
    return column_types


def idx_to_alphabet_notation(idx: int) -> str:
    letters = string.ascii_uppercase

    col_name = letters[idx % 26]
    idx = (idx // 26) - 1
    while idx >= 0:
        col_name = letters[idx % 26] + col_name
        idx = (idx // 26) - 1

    return col_name


def idx_to_file_notation(idx: int) -> str:
    return f"field{idx + 1}"


def make_result_types(
    header_values: Sequence[str | Cell],
    types: TColumnTypes,
    has_header: bool,
    missing_title_generator: Callable[[int], str] = idx_to_file_notation,
) -> TResultTypes:
    def get_value(value: str | Cell) -> Optional[str]:
        if isinstance(value, Cell):
            return str(value.value) if not value.empty else None
        return str(value)

    result_types: TResultTypes = []
    for index, t in types.items():
        header_value = get_value(header_values[index]) if has_header and len(header_values) > index else ""
        result_types.append(
            dict(
                name=generate_column_hash_name(),
                index=index,
                cast=t.bi_type.name,
                title=header_value or missing_title_generator(index),
            )
        )
    result_types.sort(key=lambda t: t["index"])
    return result_types


def guess_column_types(
    data_iter: Iterator,
    sample_lines_count: Optional[int] = None,
    cell_type_guesser: Callable[..., ParsingDataType] = guess_cell_type,
) -> TColumnTypes:
    column_types: TColumnTypes = {}
    with GenericProfiler("guess_all_rows_types"):
        rows_seen = 0
        for row in data_iter:
            if sample_lines_count and rows_seen >= sample_lines_count:
                break
            rows_seen += 1

            for col_index, value in enumerate(row):
                prev_type = column_types.get(col_index)
                if prev_type:
                    new_type = cell_type_guesser(value, prev_type.order)
                    new_cell_type = _choose_new_cell_type(prev_type=prev_type, new_type=new_type)
                else:
                    new_cell_type = cell_type_guesser(value)

                if new_cell_type:
                    column_types[col_index] = new_cell_type

    if rows_seen == 0:
        raise CsvParsingException("Too few CSV rows received.")

    return column_types


@generic_profiler("guess_types_and_header")
def guess_types_and_header(
    data_iter: Iterator,
    has_header: Optional[bool] = None,
    sample_lines_count: Optional[int] = None,
    **kwargs: Any,
) -> tuple[bool, TResultTypes]:
    if has_header is None or has_header:
        header = next(data_iter)  # assume first row is header
    else:
        header = []

    header_types = {col_index: guess_cell_type(value) for col_index, value in enumerate(header)}
    column_types = guess_column_types(data_iter, sample_lines_count, guess_cell_type)

    if has_header is None:
        has_header = guess_has_header(header_types, column_types)

    merged_column_types = merge_column_types(header_types, column_types, has_header)
    result_merged_types = make_result_types(header, merged_column_types, has_header)

    return has_header, result_merged_types


@generic_profiler("guess_types_and_header_gsheets")
def guess_types_and_header_gsheets(
    data_iter: Iterator,
    sample_lines_count: Optional[int] = None,
) -> tuple[bool, TResultTypes, TResultTypes, TResultTypes]:
    header = next(data_iter)  # assume first row is header

    header_types = {col_index: guess_cell_type_gsheet(value) for col_index, value in enumerate(header)}
    column_types = guess_column_types(data_iter, sample_lines_count, guess_cell_type_gsheet)

    has_header = guess_has_header(header_types, column_types)

    merged_column_types = merge_column_types(header_types, column_types, has_header)

    result_merged_types = make_result_types(header, merged_column_types, has_header, idx_to_alphabet_notation)
    result_header_types = make_result_types(header, header_types, True, idx_to_alphabet_notation)
    result_column_types = make_result_types(header, column_types, False, idx_to_alphabet_notation)

    return has_header, result_merged_types, result_header_types, result_column_types


@generic_profiler("guess_types_gsheets")
def guess_types_gsheets(
    data_iter: Iterator,
    sample_lines_count: Optional[int] = None,
) -> TResultTypes:
    column_types = guess_column_types(data_iter, sample_lines_count, guess_cell_type_gsheet)

    header: list[str] = []
    result_column_types = make_result_types(header, column_types, False, idx_to_alphabet_notation)

    return result_column_types


def guess_cell_type_excel(cell: dict, *args: Any, **kwargs: Any) -> ParsingDataType:
    value = cell.get("value")
    number_format = cell.get("data_type")
    assert isinstance(number_format, str)

    number_format_to_parsing_data_type_map = {
        "s": _STRING_PARSING_DATA_TYPE,
        "d": _DATETIME_PARSING_DATA_TYPE,
        "i": _INTEGER_PARSING_DATA_TYPE,
        "n": _FLOAT_PARSING_DATA_TYPE,
    }

    if value is None or value == "":
        return _NONE_PARSING_DATA_TYPE

    # Fallback to string in case of type recognition errors
    return number_format_to_parsing_data_type_map.get(number_format, _STRING_PARSING_DATA_TYPE)


def make_excel_result_types(
    header_values: Sequence[dict],
    types: TColumnTypes,
    has_header: bool,
) -> TResultTypes:
    def get_value(values: dict) -> Optional[str]:
        value: Optional[str] = values.get("value")
        return value

    result_types: TResultTypes = []
    for index, t in types.items():
        header_value = get_value(header_values[index]) if has_header and len(header_values) > index else ""
        result_types.append(
            dict(
                name=generate_column_hash_name(),
                index=index,
                cast=t.bi_type.name,
                title=header_value or "field{}".format(index + 1),
            )
        )
    result_types.sort(key=lambda t: t["index"])
    return result_types


@generic_profiler("guess_types_and_header_excel")
def guess_types_and_header_excel(
    data_iter: Iterator,
    sample_lines_count: Optional[int] = None,
) -> tuple[bool, TResultTypes, TResultTypes, TResultTypes]:
    header = next(data_iter)  # assume first row is header

    header_types = {col_index: guess_cell_type_excel(value) for col_index, value in enumerate(header)}
    column_types = guess_column_types(data_iter, sample_lines_count, guess_cell_type_excel)

    has_header = guess_has_header(header_types, column_types)

    merged_column_types = merge_column_types(header_types, column_types, has_header)

    result_merged_types = make_excel_result_types(header, merged_column_types, has_header)
    result_header_types = make_excel_result_types(header, header_types, True)
    result_column_types = make_excel_result_types(header, column_types, False)

    return has_header, result_merged_types, result_header_types, result_column_types
