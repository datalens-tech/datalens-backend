import collections
import contextvars
import types
from typing import Any

from typing_extensions import Self


ContextItem = Any
ContextItemCollection = list[ContextItem]
Context = dict[str, ContextItemCollection]
_LOG_CONTEXT = contextvars.ContextVar[Context | None]("_LOG_CONTEXT", default=None)


class ContextFormatter:
    @classmethod
    def is_context_exist(cls) -> bool:
        return _LOG_CONTEXT.get() is not None

    @classmethod
    def get_or_create_logging_context(cls) -> Context:
        context = _LOG_CONTEXT.get()

        if context is None:
            context = collections.defaultdict(list)
            _LOG_CONTEXT.set(context)

        return context


def reset_context() -> None:
    _LOG_CONTEXT.set(collections.defaultdict(list))


def put_to_context(key: str, value: Any) -> None:
    ctx = ContextFormatter.get_or_create_logging_context()
    ctx[key].append(value)


def pop_from_context(key: str) -> None:
    if not ContextFormatter.is_context_exist():
        return

    ctx = ContextFormatter.get_or_create_logging_context()
    if key not in ctx:
        return

    ctx[key].pop()
    if not ctx[key]:
        del ctx[key]


class LogContext(object):
    def __init__(self, **context: ContextItem) -> None:
        self.context = context

    def __enter__(self) -> Self:
        for key, value in self.context.items():
            put_to_context(key, value)

        return self

    def __exit__(self, exc_type: type | None, exc_val: Exception | None, exc_tb: types.TracebackType | None) -> None:
        for key in self.context:
            pop_from_context(key)


def get_log_context() -> dict[str, Any]:
    if not ContextFormatter.is_context_exist():
        return {}
    ctx = ContextFormatter.get_or_create_logging_context()
    return {key: values[-1] for key, values in ctx.items()}
