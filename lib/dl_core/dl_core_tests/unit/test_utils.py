from __future__ import annotations

import contextlib
from typing import Any

import pytest

from dl_core.utils import (
    secrepr,
    validate_hostname_or_ip_address,
)


@pytest.mark.parametrize(
    "host,valid",
    (
        ("example.01", False),
        ("example.com", True),
        ("example.com:1024", False),
        ("example.com#fragment", False),
        ("example.com/folder", False),
        ("8.8.8.8", True),
        ("8.8.8.8:8080", False),
        ("8.8.8.256", False),
        ("1::", True),
        ("e::", True),
        ("h::", False),
        ("ololo_azaza", False),
        ("$&5sdf.ru", False),
    ),
)
def test_host_validation(host, valid):
    cm = contextlib.nullcontext() if valid else pytest.raises(ValueError)

    with cm:
        validate_hostname_or_ip_address(host)


SECREPR_TESTS: tuple[tuple[Any, str], ...] = (
    (None, "None"),
    ("", "''"),
    ([1], "???<class 'list'>???"),
    ("abc", "..."),
    ("abcdefghji", "..."),
)


@pytest.mark.parametrize("value,expected", SECREPR_TESTS, ids=[str(val) for val, _ in SECREPR_TESTS])
def test_secrepr(value, expected):
    actual = secrepr(value)
    assert actual == expected
