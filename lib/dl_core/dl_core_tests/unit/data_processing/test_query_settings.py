from unittest.mock import MagicMock

import pytest

import dl_core.exc as exc
from dl_core.us_connection_base import ConnectionBase


def _make_connection(
    *,
    enabled: bool = True,
    allowed: frozenset[str] | None = None,
    forbidden: frozenset[str] = frozenset(),
) -> MagicMock:
    connection = MagicMock()
    connection.is_query_settings_enabled = enabled
    connection.query_settings_allowed_names = allowed
    connection.query_settings_forbidden_names = forbidden
    return connection


def test_validate_noop_on_empty_query_settings() -> None:
    connection = _make_connection(enabled=False)
    ConnectionBase.validate_query_settings(connection, {})


def test_validate_rejects_when_not_enabled() -> None:
    connection = _make_connection(enabled=False)
    with pytest.raises(exc.QuerySettingsNotSupportedError):
        ConnectionBase.validate_query_settings(connection, {"max_threads": "4"})


def test_validate_rejects_forbidden_setting() -> None:
    connection = _make_connection(forbidden=frozenset({"readonly"}))
    with pytest.raises(exc.QuerySettingForbiddenError):
        ConnectionBase.validate_query_settings(connection, {"readonly": "1"})


def test_validate_rejects_setting_not_in_whitelist() -> None:
    connection = _make_connection(allowed=frozenset({"max_threads"}))
    with pytest.raises(exc.QuerySettingNotAllowedError):
        ConnectionBase.validate_query_settings(connection, {"unknown_setting": "1"})


def test_validate_passes_when_whitelist_is_none() -> None:
    connection = _make_connection(allowed=None)
    ConnectionBase.validate_query_settings(connection, {"any_setting": "1"})


def test_validate_passes_when_whitelist_contains_key() -> None:
    connection = _make_connection(allowed=frozenset({"max_threads"}))
    ConnectionBase.validate_query_settings(connection, {"max_threads": "4"})


def test_validate_rejects_empty_whitelist() -> None:
    connection = _make_connection(allowed=frozenset())
    with pytest.raises(exc.QuerySettingNotAllowedError):
        ConnectionBase.validate_query_settings(connection, {"max_threads": "4"})
