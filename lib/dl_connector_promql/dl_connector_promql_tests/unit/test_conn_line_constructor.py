from typing import Any

import pytest

from dl_connector_promql.core.adapter import PromQLConnLineConstructor
from dl_connector_promql.core.constants import PromQLAuthType
from dl_connector_promql.core.target_dto import PromQLConnTargetDTO


DSN_TEMPLATE = "{dialect}://{user}:{passwd}@{host}:{port}/{db_name}"


def make_constructor(
    username: str | None = "user",
    password: str | None = "secret",
) -> PromQLConnLineConstructor:
    dto = PromQLConnTargetDTO(
        conn_id=None,
        host="localhost",
        port=9090,
        username=username,
        password=password,
        db_name="metrics",
        ca_data="",
        path="/",
        protocol="http",
        auth_type=PromQLAuthType.password,
        auth_header=None,
    )
    return PromQLConnLineConstructor(
        dsn_template=DSN_TEMPLATE,
        dialect_name="promql",
        target_dto=dto,
    )


@pytest.mark.parametrize(
    ("dto_kwargs", "expected"),
    [
        pytest.param(
            {"password": "pass word"},
            {"passwd": "pass%20word"},
            id="password-space-encoded-as-percent20",
        ),
        pytest.param(
            {"username": "my user"},
            {"user": "my%20user"},
            id="username-space-encoded-as-percent20",
        ),
        pytest.param(
            {"password": "secret123"},
            {"passwd": "secret123"},
            id="password-without-special-chars-unchanged",
        ),
        pytest.param(
            {"username": None, "password": None},
            {"user": None, "passwd": None},
            id="none-credentials-produce-none-params",
        ),
    ],
)
def test_get_dsn_params(dto_kwargs: dict[str, Any], expected: dict[str, Any]) -> None:
    params = make_constructor(**dto_kwargs)._get_dsn_params()
    for key, value in expected.items():
        assert params[key] == value


@pytest.mark.parametrize(
    ("password", "forbidden_substring"),
    [
        pytest.param("pass word", "+", id="space-not-encoded-as-plus"),
    ],
)
def test_password_encoding_forbidden_substrings(password: str, forbidden_substring: str) -> None:
    params = make_constructor(password=password)._get_dsn_params()
    assert forbidden_substring not in params["passwd"]
