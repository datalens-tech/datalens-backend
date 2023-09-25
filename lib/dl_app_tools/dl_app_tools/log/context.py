# coding: utf-8
from __future__ import annotations

from collections import defaultdict
import contextvars
import sys

import six


_LOG_CONTEXT = contextvars.ContextVar("_LOG_CONTEXT", default=None)


def force_unicode(obj):  # type: ignore  # TODO: fix
    if not isinstance(obj, six.string_types):
        obj = repr(obj)
    return obj.decode(sys.getdefaultencoding())


class ContextFormatter:
    @classmethod
    def is_context_exist(cls):  # type: ignore  # TODO: fix
        return _LOG_CONTEXT.get() is not None

    @classmethod
    def get_or_create_logging_context(cls):  # type: ignore  # TODO: fix
        if not cls.is_context_exist():
            _LOG_CONTEXT.set(defaultdict(list))  # type: ignore  # TODO: fix
        return _LOG_CONTEXT.get()


def reset_context():  # type: ignore  # TODO: fix
    _LOG_CONTEXT.set(defaultdict(list))  # type: ignore  # TODO: fix


def put_to_context(key, value):  # type: ignore  # TODO: fix
    ctx = ContextFormatter.get_or_create_logging_context()
    ctx[key].append(value)


def pop_from_context(key):  # type: ignore  # TODO: fix
    if not ContextFormatter.is_context_exist():
        return
    ctx = ContextFormatter.get_or_create_logging_context()
    if key not in ctx:
        return
    value = ctx[key].pop()
    if not ctx[key]:
        del ctx[key]
    return value


class LogContext(object):
    context = None

    def __init__(self, **context):  # type: ignore  # TODO: fix
        self.context = context

    def __enter__(self):  # type: ignore  # TODO: fix
        for key, value in self.context.items():  # type: ignore  # TODO: fix
            put_to_context(key, value)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):  # type: ignore  # TODO: fix
        for key in self.context:  # type: ignore  # TODO: fix
            pop_from_context(key)
        return False


log_context = LogContext


def get_log_context():  # type: ignore  # TODO: fix
    if not ContextFormatter.is_context_exist():
        return {}
    ctx = ContextFormatter.get_or_create_logging_context()
    return {key: values[-1] for key, values in ctx.items()}
