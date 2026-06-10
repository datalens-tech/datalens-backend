from collections.abc import Callable
import datetime
import decimal
import logging
from typing import Any

import dl_constants

LOGGER = logging.getLogger(__name__)


class ParameterValueCoercionError(ValueError):
    """Raised when a raw value cannot be coerced to the declared parameter type.

    Subclasses ``ValueError`` so existing ``except (ValueError, TypeError)``
    handlers (e.g. in ``make_literal_node``) keep working unchanged.
    """


def parse_datetime_isoformat(val: str) -> datetime.datetime:
    val = val.replace(" ", "T")
    if val.endswith("Z"):
        val = val[:-1] + "+00:00"
    return datetime.datetime.fromisoformat(val)


def coerce_string(raw: Any) -> str:
    if not isinstance(raw, str):
        raise ParameterValueCoercionError(f"Expected a string, got {type(raw).__name__}")
    return raw


def coerce_integer(raw: Any) -> int:
    if isinstance(raw, bool):
        raise ParameterValueCoercionError("Boolean is not a valid integer")
    if isinstance(raw, (float, decimal.Decimal)):
        try:
            truncated = int(raw)
        except (ValueError, OverflowError) as e:
            # nan / inf cannot be represented as an integer
            raise ParameterValueCoercionError(f"Cannot coerce {raw!r} to integer") from e
        if truncated != raw:
            LOGGER.warning("Truncating non-integral value to integer: %s", raw)
        return truncated
    try:
        return int(raw)
    except (ValueError, TypeError) as e:
        raise ParameterValueCoercionError(f"Cannot coerce {raw!r} to integer") from e


def coerce_float(raw: Any) -> float:
    if isinstance(raw, bool):
        raise ParameterValueCoercionError("Boolean is not a valid float")
    try:
        return float(raw)
    except (ValueError, TypeError) as e:
        raise ParameterValueCoercionError(f"Cannot coerce {raw!r} to float") from e


def coerce_boolean(raw: Any) -> bool:
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str):
        lowered = raw.lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
        raise ParameterValueCoercionError(f"Invalid boolean literal {raw!r}")
    raise ParameterValueCoercionError(f"Cannot coerce {type(raw).__name__} to boolean")


def coerce_date(raw: Any) -> datetime.date:
    if isinstance(raw, datetime.datetime):
        value = raw
    elif isinstance(raw, datetime.date):
        return raw
    elif isinstance(raw, str):
        try:
            value = parse_datetime_isoformat(raw)
        except (ValueError, TypeError) as e:
            raise ParameterValueCoercionError(f"Cannot coerce {raw!r} to date") from e
    else:
        raise ParameterValueCoercionError(f"Cannot coerce {type(raw).__name__} to date")

    if value.hour or value.minute or value.second or value.microsecond:
        LOGGER.warning("Truncating datetime with nonzero time to date: %s", raw)
    return value.date()


def coerce_datetime(raw: Any) -> datetime.datetime:
    if isinstance(raw, datetime.datetime):
        return raw
    if isinstance(raw, str):
        try:
            return parse_datetime_isoformat(raw)
        except (ValueError, TypeError) as e:
            raise ParameterValueCoercionError(f"Cannot coerce {raw!r} to datetime") from e
    raise ParameterValueCoercionError(f"Cannot coerce {type(raw).__name__} to datetime")


def coerce_datetime_tz(raw: Any) -> datetime.datetime:
    value = coerce_datetime(raw)
    # Offset-less datetimes are interpreted as UTC.
    if value.tzinfo is None:
        value = value.replace(tzinfo=datetime.UTC)
    return value


# Per-type coercers, shared between the templated-source parameter path
# (``DatasetComponentAccessor.get_parameter_values_from_specs``) and the formula
# parameter path (``dl_query_processing.compilation.helpers.make_literal_node``)
# so both validate parameter values identically.
_COERCERS: dict[dl_constants.UserDataType, Callable[[Any], Any]] = {
    dl_constants.UserDataType.string: coerce_string,
    dl_constants.UserDataType.integer: coerce_integer,
    dl_constants.UserDataType.float: coerce_float,
    dl_constants.UserDataType.boolean: coerce_boolean,
    dl_constants.UserDataType.date: coerce_date,
    dl_constants.UserDataType.datetime: coerce_datetime,
    dl_constants.UserDataType.genericdatetime: coerce_datetime,
    dl_constants.UserDataType.datetimetz: coerce_datetime_tz,
}


def coerce_value(raw: Any, user_data_type: dl_constants.UserDataType) -> Any:
    """Coerce ``raw`` (possibly string-encoded) to the native value for ``user_data_type``.

    Raises ``ParameterValueCoercionError`` if the value cannot be represented as
    ``user_data_type``.
    """
    coercer = _COERCERS.get(user_data_type)
    if coercer is None:
        raise ParameterValueCoercionError(f"Unsupported parameter type {user_data_type.name}")
    return coercer(raw)
