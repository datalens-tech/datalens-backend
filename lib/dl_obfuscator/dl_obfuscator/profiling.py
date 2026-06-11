from contextvars import ContextVar
import logging

import attr


@attr.s
class LogFormatProfilingContext:
    total_format_time: float = attr.ib(default=0.0)
    obfuscation_time: float = attr.ib(default=0.0)
    call_count: int = attr.ib(default=0)


_log_format_profiling: ContextVar[LogFormatProfilingContext | None] = ContextVar("_log_format_profiling", default=None)


def init_log_format_profiling() -> None:
    _log_format_profiling.set(LogFormatProfilingContext())


def get_log_format_profiling() -> LogFormatProfilingContext | None:
    return _log_format_profiling.get()


def clear_log_format_profiling() -> None:
    _log_format_profiling.set(None)


_PROFILING_LOGGER = logging.getLogger("dl_obfuscator.profiling")


def dump_log_format_profiling() -> None:
    profiling = _log_format_profiling.get()
    if profiling is None:
        return
    if profiling.call_count == 0:
        return
    total_ms = round(profiling.total_format_time * 1000, 3)
    obf_ms = round(profiling.obfuscation_time * 1000, 3)
    call_count = profiling.call_count
    token = _log_format_profiling.set(None)
    try:
        _PROFILING_LOGGER.info(
            "DL_LOG_FORMAT_PROFILING format_time_ms=%.3f obfuscation_time_ms=%.3f calls=%d",
            total_ms,
            obf_ms,
            call_count,
            extra={
                "event_code": "dl_log_format_profiling",
                "log_format_time_ms": total_ms,
                "obfuscation_time_ms": obf_ms,
                "log_format_call_count": call_count,
            },
        )
    finally:
        _log_format_profiling.reset(token)
