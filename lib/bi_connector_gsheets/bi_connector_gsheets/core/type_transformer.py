import datetime
from typing import Optional

from bi_constants.enums import BIType, ConnectionType as CT

from bi_core.db.conversion_base import TypeCaster, TypeTransformer, make_native_type


def preparse_gsheets_date_value(
        value: Optional[str], strict: bool = False,
) -> Optional[tuple[int, ...]]:
    """
    >>> parse_date_value(None)
    >>> parse_date_value('Date(1998,5,5)')
    (1998, 5, 5)
    >>> parse_date_value('Date(2021,2,12,13,14,15)')
    (2021, 2, 12, 13, 14, 15)
    """
    if not value:
        return None
    # value = value.strip()  # should not be needed, hopefully
    prefix = 'Date('
    suffix = ')'
    if not (value.startswith(prefix) and value.endswith(suffix)):
        if strict:
            raise ValueError(f'Unexpected gsheets date value: {value!r}')
        return None
    value_main = value[len(prefix):-len(suffix)]
    value_pieces = value_main.split(',')
    # value_pieces = [piece.strip() for piece in value_pieces]  # should not be needed, hopefully
    if not all(piece.isdigit() for piece in value_pieces):
        if strict:
            raise ValueError(f'Non-numeric gsheets date value: {value!r}')
        return None
    result = [int(piece) for piece in value_pieces]
    if len(result) >= 2:
        # GSheets `Date()` imitates a JS date, which 0-indexed month.
        # https://developers.google.com/chart/interactive/docs/datesandtimes
        result[1] += 1
    return tuple(result)


def parse_gsheets_date(value: Optional[str], strict: bool = False) -> datetime.date:
    pieces = preparse_gsheets_date_value(value=value, strict=strict)
    if not pieces:
        return None  # type: ignore  # TODO: fix
    return datetime.date(*pieces)


def parse_gsheets_datetime(value: Optional[str], strict: bool = False) -> datetime.datetime:
    pieces = preparse_gsheets_date_value(value=value, strict=strict)
    if not pieces:
        return None  # type: ignore  # TODO: fix
    return datetime.datetime(*pieces)  # type: ignore  # TODO: fix


class GSheetsDateTypeCaster(TypeCaster):
    cast_func = parse_gsheets_date


class GSheetsDatetimeTypeCaster(TypeCaster):
    cast_func = parse_gsheets_datetime


class GSheetsGenericDatetimeTypeCaster(TypeCaster):
    cast_func = parse_gsheets_datetime


class GSheetsTypeTransformer(TypeTransformer):
    conn_type = CT.gsheets
    native_to_user_map = {
        # make_native_type(CT.gsheets, 'number'): BIType.integer,
        make_native_type(CT.gsheets, 'number'): BIType.float,
        make_native_type(CT.gsheets, 'string'): BIType.string,
        make_native_type(CT.gsheets, 'date'): BIType.date,
        make_native_type(CT.gsheets, 'datetime'): BIType.genericdatetime,
        make_native_type(CT.gsheets, 'boolean'): BIType.boolean,
        # make_native_type(CT.gsheets, 'unsupported'): BIType.unsupported,
    }
    user_to_native_map = dict([
        (bi_type, native_type) for native_type, bi_type in native_to_user_map.items()
    ] + [
        (BIType.datetime, make_native_type(CT.gsheets, 'datetime')),
    ])
    casters = {
        **TypeTransformer.casters,
        BIType.date: GSheetsDateTypeCaster(),
        BIType.datetime: GSheetsDatetimeTypeCaster(),
        BIType.genericdatetime: GSheetsGenericDatetimeTypeCaster(),
    }
