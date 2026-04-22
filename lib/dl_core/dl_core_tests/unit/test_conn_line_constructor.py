from typing import Any

import pytest

from dl_core.connection_executors.adapters.adapters_base_sa_classic import ClassicSQLConnLineConstructor
from dl_core.connection_executors.models.connection_target_dto_base import BaseSQLConnTargetDTO


DSN_TEMPLATE = "{dialect}://{user}:{passwd}@{host}:{port}/{db_name}"


def make_constructor(
    username: str = "user",
    password: str = "secret",
    db_name: str = "mydb",
) -> ClassicSQLConnLineConstructor:
    dto = BaseSQLConnTargetDTO(
        conn_id=None,
        host="localhost",
        port=5432,
        username=username,
        password=password,
        db_name=db_name,
    )
    return ClassicSQLConnLineConstructor(
        dsn_template=DSN_TEMPLATE,
        dialect_name="postgresql",
        target_dto=dto,
    )


@pytest.mark.parametrize(
    ("dto_kwargs", "dsn_kwargs", "param_key", "expected"),
    [
        pytest.param(
            {"password": "pass word"},
            {},
            "passwd",
            "pass%20word",
            id="password-space-encoded-as-percent20",
        ),
        pytest.param(
            {"username": "my user"},
            {},
            "user",
            "my%20user",
            id="username-space-encoded-as-percent20",
        ),
        pytest.param(
            {"password": "secret123"},
            {},
            "passwd",
            "secret123",
            id="password-without-special-chars-unchanged",
        ),
        pytest.param(
            {"db_name": "my db"},
            {},
            "db_name",
            "my%20db",
            id="db-name-space-encoded-as-percent20",
        ),
        pytest.param(
            {"db_name": "my*db"},
            {"safe_db_symbols": ("*",)},
            "db_name",
            "my*db",
            id="db-name-safe-symbols-not-encoded",
        ),
    ],
)
def test_get_dsn_params(
    dto_kwargs: dict[str, Any],
    dsn_kwargs: dict[str, Any],
    param_key: str,
    expected: str,
) -> None:
    params = make_constructor(**dto_kwargs)._get_dsn_params(**dsn_kwargs)
    assert params[param_key] == expected


@pytest.mark.parametrize(
    ("password", "forbidden_substring"),
    [
        pytest.param("pass word", "+", id="space-not-encoded-as-plus"),
    ],
)
def test_password_encoding_forbidden_substrings(password: str, forbidden_substring: str) -> None:
    params = make_constructor(password=password)._get_dsn_params()
    assert forbidden_substring not in params["passwd"]
