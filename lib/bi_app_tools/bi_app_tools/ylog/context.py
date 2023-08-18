# coding: utf-8
"""
Контекстное логирование — прозрачное для пользователя добавление в сообщения
лога произвольной строчки -- ID контекста (например веб-запроса). Потом в логе
легко отличать строки, сделанные в разных запросах.

Использование:

    from ylog import log_req_id

    @log_req_id
    def func_that_logs_something():
        log.info('something')

    ...

    func_that_logs_something(req_id=some_id)

В логе будет написано:

    время logger INFO something; req_id=12345
"""

from __future__ import annotations

import contextvars
import sys
from collections import defaultdict
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
    """
    Публичная функция, чтобы положить ключ-значение в контекст.
    """
    ctx = ContextFormatter.get_or_create_logging_context()
    ctx[key].append(value)


def pop_from_context(key):  # type: ignore  # TODO: fix
    """
    Публичная функция, чтобы достать значение с вершины стека для ключа
    из контекста.

    Перед использованием функции убедитесь, что вызов не происходит внутри
    контекста менеджера log_context и не манипулирует значениями ключей,
    добавленных вызовом менеджера.
    """
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
        return False  # не подавлять исключений


log_context = LogContext


def get_log_context():  # type: ignore  # TODO: fix
    if not ContextFormatter.is_context_exist():
        return {}
    ctx = ContextFormatter.get_or_create_logging_context()
    # В logging_context каждому ключу соответствует стек из значений,
    # нам нужно значение с вершины стека.
    return {key: values[-1] for key, values in ctx.items()}
