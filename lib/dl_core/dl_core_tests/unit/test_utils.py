from __future__ import annotations

from typing import Any

import pytest

from dl_core.utils import (
    secrepr,
    secrepr_db_url,
    validate_hostname_or_ip_address,
)


@pytest.mark.parametrize(
    "host",
    [
        "example.com",
        "8.8.8.8",
        "1::",
        "e::",
    ],
)
def test_host_validation_valid(host):
    validate_hostname_or_ip_address(host)


@pytest.mark.parametrize(
    ("host", "expected_error"),
    [
        ("example.01", "TLD must be not all-numeric"),
        ("example.com:1024", "Not a valid domain name"),
        ("example.com#fragment", "Not a valid domain name"),
        ("example.com/folder", "Not a valid domain name"),
        ("8.8.8.8:8080", "Not a valid domain name"),
        ("8.8.8.256", "TLD must be not all-numeric"),
        ("h::", "Not a valid domain name"),
        ("ololo_azaza", "Not a valid domain name"),
        ("$&5sdf.ru", "Not a valid domain name"),
    ],
)
def test_host_validation_invalid(host, expected_error):
    with pytest.raises(ValueError, match=expected_error):
        validate_hostname_or_ip_address(host)


SECREPR_TESTS: tuple[tuple[Any, str], ...] = (
    (None, "None"),
    ("", "''"),
    ([1], "???<class 'list'>???"),
    ("abc", "..."),
    ("abcdefghji", "..."),
)


@pytest.mark.parametrize(("value", "expected"), SECREPR_TESTS, ids=[str(val) for val, _ in SECREPR_TESTS])
def test_secrepr(value, expected):
    actual = secrepr(value)
    assert actual == expected


SECREPR_DB_URL_TESTS: tuple[tuple[Any, str], ...] = (
    (None, "None"),
    ("", "''"),
    ([1], "???<class 'list'>???"),
    (
        "postgresql://user:password@localhost:5432/db",
        "'postgresql://{user}:{password}@localhost:5432/db'",
    ),
    (
        "postgresql://user:password@localhost:5432/db?sslmode=require",
        "'postgresql://{user}:{password}@localhost:5432/db?sslmode=require'",
    ),
    (
        "postgresql://user@localhost:5432/db",
        "'postgresql://{user}@localhost:5432/db'",
    ),
    (
        "postgresql://localhost:5432/db",
        "'postgresql://localhost:5432/db'",
    ),
    (
        "postgresql+asyncpg://us:p%40ss%3Aword@localhost:5432/us-db",
        "'postgresql+asyncpg://{user}:{password}@localhost:5432/us-db'",
    ),
)


@pytest.mark.parametrize(("value", "expected"), SECREPR_DB_URL_TESTS, ids=[str(val) for val, _ in SECREPR_DB_URL_TESTS])
def test_secrepr_db_url(value, expected):
    actual = secrepr_db_url(value)
    assert actual == expected
