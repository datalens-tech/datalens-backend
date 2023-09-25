from __future__ import annotations

import datetime

import ciso8601  # type: ignore


def parse_datetime(value: str) -> datetime.datetime:
    """
    Parse an ISO8601 datetime value, e.g. from API parameters.

    See also: `bi_formula.utils.datetime.parse_dt_string`

    >>> parse_datetime('2020-01-02T03:04:05')
    datetime.datetime(2020, 1, 2, 3, 4, 5)
    >>> parse_datetime('2020-01-02T03:04:05.678')
    datetime.datetime(2020, 1, 2, 3, 4, 5, 678000)
    >>> parse_datetime('2020-01-02T03:04:05+12:34')
    datetime.datetime(2020, 1, 2, 3, 4, 5, tzinfo=datetime.timezone(datetime.timedelta(seconds=45240)))
    >>> try:
    ...     parse_datetime('2020-01-02T03:04:65')
    ... except ValueError:
    ...     pass
    >>> parse_datetime('2020-01-02')
    datetime.datetime(2020, 1, 2, 0, 0)
    """
    return ciso8601.parse_datetime(value)
