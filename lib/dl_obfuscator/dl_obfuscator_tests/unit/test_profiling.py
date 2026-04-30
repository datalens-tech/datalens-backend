import logging

import pytest

from dl_obfuscator.profiling import (
    LogFormatProfilingContext,
    clear_log_format_profiling,
    dump_log_format_profiling,
    get_log_format_profiling,
    init_log_format_profiling,
)


def test_accumulation() -> None:
    ctx = LogFormatProfilingContext()
    ctx.total_format_time += 0.001
    ctx.total_format_time += 0.002
    ctx.obfuscation_time += 0.0005
    ctx.call_count += 1
    ctx.call_count += 1
    assert ctx.total_format_time == pytest.approx(0.003)
    assert ctx.obfuscation_time == pytest.approx(0.0005)
    assert ctx.call_count == 2


def test_init_and_clear() -> None:
    assert get_log_format_profiling() is None
    try:
        init_log_format_profiling()
        assert get_log_format_profiling() is not None
    finally:
        clear_log_format_profiling()
    assert get_log_format_profiling() is None


def test_mutations_visible_via_get() -> None:
    try:
        init_log_format_profiling()
        ctx = get_log_format_profiling()
        ctx.total_format_time += 0.005
        ctx.call_count += 3
        same_ctx = get_log_format_profiling()
        assert same_ctx.total_format_time == pytest.approx(0.005)
        assert same_ctx.call_count == 3
    finally:
        clear_log_format_profiling()


def test_dump_emits_log_record(caplog: pytest.LogCaptureFixture) -> None:
    try:
        init_log_format_profiling()
        ctx = get_log_format_profiling()
        ctx.total_format_time = 0.045321
        ctx.obfuscation_time = 0.012108
        ctx.call_count = 287

        with caplog.at_level(logging.INFO, logger="dl_obfuscator.profiling"):
            dump_log_format_profiling()

        assert len(caplog.records) == 1
        assert (
            caplog.records[0].message
            == "DL_LOG_FORMAT_PROFILING format_time_ms=45.321 obfuscation_time_ms=12.108 calls=287"
        )
    finally:
        clear_log_format_profiling()


def test_dump_noop_when_not_initialized(caplog: pytest.LogCaptureFixture) -> None:
    clear_log_format_profiling()
    with caplog.at_level(logging.INFO, logger="dl_obfuscator.profiling"):
        dump_log_format_profiling()
    assert len(caplog.records) == 0


def test_dump_with_zero_values(caplog: pytest.LogCaptureFixture) -> None:
    try:
        init_log_format_profiling()
        with caplog.at_level(logging.INFO, logger="dl_obfuscator.profiling"):
            dump_log_format_profiling()
        assert len(caplog.records) == 0  # call_count == 0 → dump is skipped
    finally:
        clear_log_format_profiling()
