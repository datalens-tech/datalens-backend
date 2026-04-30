import logging

import mock
import pytest

import dl_json
import dl_logging
from dl_obfuscator.profiling import (
    clear_log_format_profiling,
    get_log_format_profiling,
    init_log_format_profiling,
)


@pytest.fixture(name="record")
def fixture_record() -> logging.LogRecord:
    return logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="test",
        args=(),
        exc_info=None,
    )


def test_deploy_json_formatter(
    record: logging.LogRecord,
) -> None:
    formatter = dl_logging.DeployJsonFormatter()

    raw_result = formatter.format(record)

    result = dl_json.loads_str(raw_result)
    assert result == {
        "message": "test",
        "level": "INFO",
        "@fields": {
            "std": {
                "funcName": None,
                "lineno": 1,
                "name": "test",
            },
        },
    }


def test_stdout_formatter_is_deploy(
    monkeypatch: pytest.MonkeyPatch,
    record: logging.LogRecord,
) -> None:
    monkeypatch.setenv("DEPLOY_BOX_ID", "1")
    formatter = dl_logging.StdoutFormatter(fmt=None, datefmt=None, style="%", validate=True, defaults=None)

    raw_result = formatter.format(record)

    result = dl_json.loads_str(raw_result)
    assert result == {
        "message": "test",
        "level": 20,
        "levelStr": "INFO",
        "loggerName": "test",
        "@fields": {
            "std": {
                "funcName": None,
                "lineno": 1,
                "name": "test",
            },
        },
    }


def test_stdout_formatter_is_not_deploy(
    monkeypatch: pytest.MonkeyPatch,
    record: logging.LogRecord,
) -> None:
    formatter = dl_logging.StdoutFormatter(fmt=None, datefmt=None, style="%", validate=True, defaults=None)

    raw_result = formatter.format(record)

    result = dl_json.loads_str(raw_result)
    assert result == {
        "name": "test",
        "levelname": "INFO",
        "exc_info": None,
        "lineno": 1,
        "funcName": None,
        "timestamp": mock.ANY,
        "timestampns": mock.ANY,
        "isotimestamp": mock.ANY,
        "message": "test",
        "pid": mock.ANY,
        "exc_type": None,
    }


def test_format_accumulates_profiling(record: logging.LogRecord) -> None:
    try:
        init_log_format_profiling()
        formatter = dl_logging.StdoutFormatter(fmt=None, datefmt=None, style="%", validate=True, defaults=None)

        formatter.format(record)
        formatter.format(record)

        ctx = get_log_format_profiling()
        assert ctx is not None
        assert ctx.total_format_time > 0
        assert ctx.call_count == 2
        assert ctx.obfuscation_time == 0.0
    finally:
        clear_log_format_profiling()


def test_format_works_without_profiling(record: logging.LogRecord) -> None:
    clear_log_format_profiling()
    formatter = dl_logging.StdoutFormatter(fmt=None, datefmt=None, style="%", validate=True, defaults=None)
    formatter.format(record)
    assert get_log_format_profiling() is None
