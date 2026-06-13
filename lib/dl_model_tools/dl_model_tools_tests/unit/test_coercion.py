import datetime
import decimal
import logging

import pytest

import dl_constants
import dl_model_tools


@pytest.mark.parametrize(
    ("raw", "user_type", "expected"),
    [
        # integer: native and string-encoded both accepted; integral floats too
        (2, dl_constants.UserDataType.integer, 2),
        ("2", dl_constants.UserDataType.integer, 2),
        ("-1", dl_constants.UserDataType.integer, -1),
        (2.0, dl_constants.UserDataType.integer, 2),
        # non-integral floats / decimals are truncated (with a warning), as int() does
        (2.5, dl_constants.UserDataType.integer, 2),
        (decimal.Decimal("2.5"), dl_constants.UserDataType.integer, 2),
        # float
        (3.14, dl_constants.UserDataType.float, 3.14),
        ("3.14", dl_constants.UserDataType.float, 3.14),
        ("2", dl_constants.UserDataType.float, 2.0),
        # boolean: native bool and the lowercase string forms
        (True, dl_constants.UserDataType.boolean, True),
        ("true", dl_constants.UserDataType.boolean, True),
        ("false", dl_constants.UserDataType.boolean, False),
        # string: identity
        ("anything", dl_constants.UserDataType.string, "anything"),
        # date / datetime
        ("2022-04-25", dl_constants.UserDataType.date, datetime.date(2022, 4, 25)),
        ("2022-04-25T10:11:12", dl_constants.UserDataType.datetime, datetime.datetime(2022, 4, 25, 10, 11, 12)),
        ("2022-04-25 10:11:12", dl_constants.UserDataType.genericdatetime, datetime.datetime(2022, 4, 25, 10, 11, 12)),
        # datetimetz: offset-less is interpreted as UTC, explicit offset is preserved
        (
            "2022-04-25T10:11:12",
            dl_constants.UserDataType.datetimetz,
            datetime.datetime(2022, 4, 25, 10, 11, 12, tzinfo=datetime.UTC),
        ),
        (
            "2022-04-25T10:11:12+03:00",
            dl_constants.UserDataType.datetimetz,
            datetime.datetime(2022, 4, 25, 10, 11, 12, tzinfo=datetime.timezone(datetime.timedelta(hours=3))),
        ),
    ],
)
def test_coerce_value_accepts_valid(raw, user_type, expected):
    assert dl_model_tools.coerce_value(raw, user_type) == expected


@pytest.mark.parametrize(
    ("raw", "user_type"),
    [
        # the bounty payloads must be rejected for non-string types
        ("0 OR 1=1 --", dl_constants.UserDataType.integer),
        ("-1 UNION SELECT 999, version() --", dl_constants.UserDataType.integer),
        # non-finite floats cannot be represented as an integer
        (float("nan"), dl_constants.UserDataType.integer),
        (float("inf"), dl_constants.UserDataType.integer),
        ("0 OR 1=1 --", dl_constants.UserDataType.float),
        ("0 OR 1=1 --", dl_constants.UserDataType.boolean),
        ("not-a-date", dl_constants.UserDataType.date),
        # bools are not integers/floats
        (True, dl_constants.UserDataType.integer),
        (True, dl_constants.UserDataType.float),
        # non-coercible types into string
        (5, dl_constants.UserDataType.string),
    ],
)
def test_coerce_value_rejects_invalid(raw, user_type):
    with pytest.raises(dl_model_tools.ParameterValueCoercionError):
        dl_model_tools.coerce_value(raw, user_type)


def test_coerce_value_rejects_unsupported_type():
    with pytest.raises(dl_model_tools.ParameterValueCoercionError):
        dl_model_tools.coerce_value("x", dl_constants.UserDataType.geopoint)


def test_coerce_integer_warns_on_truncation(caplog):
    with caplog.at_level(logging.WARNING):
        result = dl_model_tools.coerce_value(2.5, dl_constants.UserDataType.integer)

    assert result == 2
    assert "Truncating non-integral value to integer" in caplog.text


def test_coerce_integer_does_not_warn_on_integral(caplog):
    with caplog.at_level(logging.WARNING):
        result = dl_model_tools.coerce_value(2.0, dl_constants.UserDataType.integer)

    assert result == 2
    assert "Truncating" not in caplog.text


def test_coerce_date_warns_on_time_truncation(caplog):
    with caplog.at_level(logging.WARNING):
        result = dl_model_tools.coerce_value("2022-04-25T10:11:12", dl_constants.UserDataType.date)

    assert result == datetime.date(2022, 4, 25)
    assert "Truncating datetime with nonzero time to date" in caplog.text


def test_coerce_date_does_not_warn_without_time(caplog):
    with caplog.at_level(logging.WARNING):
        result = dl_model_tools.coerce_value("2022-04-25", dl_constants.UserDataType.date)

    assert result == datetime.date(2022, 4, 25)
    assert "Truncating" not in caplog.text
