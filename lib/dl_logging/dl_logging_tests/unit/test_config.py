import pytest

import dl_logging


def test_configure_logging_from_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOGGING__APP_NAME", "test")
    dl_logging.config.configure_logging_from_settings()
