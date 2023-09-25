# coding:utf-8

from __future__ import annotations

import json
import logging
import os

from .context import get_log_context


IS_DEPLOY = "DEPLOY_BOX_ID" in os.environ

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


def get_record_extra(record):  # type: ignore  # TODO: fix
    return {key: value for key, value in vars(record).items() if key not in DEFAULT_RECORD_ATTRS}


class JsonFormatter(logging.Formatter):
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

    def format(self, record):  # type: ignore  # TODO: fix
        record.message = record.getMessage()

        log_data = {
            "message": record.message,
            "level": record.levelname,
        }
        if IS_DEPLOY:
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

        context = {}
        log_context_fields = record.log_context if hasattr(record, "log_context") else get_log_context()
        context.update(log_context_fields)
        context.update(get_record_extra(record))
        if context:
            fields["context"] = context

        if fields:
            log_data["@fields"] = fields

        return json.dumps(log_data, default=repr)

    def _get_standard_fields(self, record):  # type: ignore  # TODO: fix
        return {field: getattr(record, field) for field in self.LOG_RECORD_USEFUL_FIELDS if hasattr(record, field)}
