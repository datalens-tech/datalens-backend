
import datetime

import pytz


DATE_NULL = '0000-00-00'
DATETIME_NULL = '0000-00-00 00:00:00'

# timezones missing in `pytz`
TZ_PATCHES = {
    # Technically, this is non-daylight-savings timezone only,
    # with 'MSD' being daylight-savings timezone,
    # but in practice, it is used interchangeably.
    'MSK': 'Europe/Moscow',
}


def parse_timezone(tz):
    tz = TZ_PATCHES.get(tz, tz)
    try:
        return pytz.timezone(tz)
    except pytz.UnknownTimeZoneError:
        raise Exception('Unknown timezone from CH', tz)


def parse_date(value):
    if value is None or value == DATE_NULL:
        return None
    return datetime.datetime.strptime(value, '%Y-%m-%d').date()


def parse_datetime(value, tzinfo=None):
    if value is None or value == DATETIME_NULL:
        return None
    result = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
    if tzinfo is not None:
        result = tzinfo.localize(result)
    return result


def parse_datetime64(value, tzinfo=None):
    if value is None or value == DATETIME_NULL:
        return None

    # CH datetime64 contains up to 9 digits after dot, so let's cut it to microseconds
    # len('2020-01-01 12:34:56.123456') == 26
    # maybe TODO: use pandas datetime64 type for nanoseconds handling
    result = datetime.datetime.fromisoformat(value[:26])

    if tzinfo is not None:
        result = tzinfo.localize(result)
    return result
