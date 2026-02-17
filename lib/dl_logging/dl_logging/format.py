import json
import logging
import os
from typing import Any

import statcommons.logs

import dl_logging.context as context
from dl_obfuscator import (
    ObfuscationContext,
    get_request_obfuscation_engine,
)


def is_deploy() -> bool:
    return "DEPLOY_BOX_ID" in os.environ


# To catch the 'extra'-added attributes.
DEFAULT_RECORD_ATTRS = frozenset(
    (
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        # Added around here:
        "message",
        # Around here and in BI:
        "log_context",
        "dlog_context",
    )
)


def get_record_extra(record: logging.LogRecord) -> dict[str, Any]:
    return {key: value for key, value in vars(record).items() if key not in DEFAULT_RECORD_ATTRS}


class DeployJsonFormatter(logging.Formatter):
    """
    Formatting log records as JSON.

    Message will be formatted as JSON with the following structure:
    {
        "msg": "message",
        "stackTrace": "your very long stacktrace \n with newlines \n and all the things",
        "level": "WARN",
        "@fields": {
            "std": {
                "funcName": "get_values",
                "lineno": 274,
                "name": "mymicroservice.dao.utils",
            },
            "context": {
                "foo": "qwerty",
                "bar": 42,
            }
        }
    }

    Dict @fields.context will contain all the fields from log_context.
    """

    LOG_RECORD_USEFUL_FIELDS = ("funcName", "lineno", "name")

    def format(self, record: logging.LogRecord) -> str:
        record.message = record.getMessage()

        log_data: dict[str, Any] = {
            "message": record.message,
            "level": record.levelname,
        }
        if is_deploy():
            log_data["levelStr"] = record.levelname
            log_data["loggerName"] = record.name
            log_data["level"] = record.levelno

        if record.exc_info:
            exc = logging.Formatter.formatException(self, record.exc_info)
            log_data["stackTrace"] = exc

        fields = {}

        standard_fields = self._get_standard_fields(record)
        if standard_fields:
            fields["std"] = standard_fields

        context_data = {}
        log_context_fields = record.log_context if hasattr(record, "log_context") else context.get_log_context()
        context_data.update(log_context_fields)
        context_data.update(get_record_extra(record))
        if context_data:
            fields["context"] = context_data

        if fields:
            log_data["@fields"] = fields

        return json.dumps(log_data, default=repr)

    def _get_standard_fields(self, record: logging.LogRecord) -> dict[str, Any]:
        return {field: getattr(record, field) for field in self.LOG_RECORD_USEFUL_FIELDS if hasattr(record, field)}


JsonFormatter = statcommons.logs.JsonExtFormatter


class StdoutFormatter(logging.Formatter):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.deploy_json_formatter = DeployJsonFormatter()
        self.json_formatter = JsonFormatter()

    def format(self, record: logging.LogRecord) -> str:
        if is_deploy():
            result = self.deploy_json_formatter.format(record)
        else:
            result = self.json_formatter.format(record)

        engine = get_request_obfuscation_engine()
        if engine is not None:
            result = engine.obfuscate(result, ObfuscationContext.LOGS)

        return result