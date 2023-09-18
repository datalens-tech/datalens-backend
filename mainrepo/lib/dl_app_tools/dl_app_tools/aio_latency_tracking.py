from __future__ import annotations

import asyncio
import logging
import math
import time
from typing import Optional

LOGGER = logging.getLogger(__name__)


class LatencyTracker:
    bins_base: float = 1.5  # logarithmic binning exponent
    sleep_interval_sec: float = 100 / 1000
    stats_log_interval_sec: float = 5.0
    min_warn_interval_sec: float = 2.5

    def __init__(self) -> None:
        # duration lower bound (milliseconds) -> count
        self._bins: dict[int, int] = {}
        self._last_log_time = time.monotonic()
        self._logger = LOGGER.getChild(self.__class__.__name__)
        self._task: Optional[asyncio.Task] = None

    def _to_bin(self, value: float) -> int:
        return int(self.bins_base ** math.floor(math.log(value) / math.log(self.bins_base)))

    def _log_duration(self, duration_sec: float) -> None:
        duration_msec = duration_sec * 1000
        duration_bin = self._to_bin(duration_msec)
        self._bins[duration_bin] = self._bins.setdefault(duration_bin, 0) + 1

    def _log_stats(self) -> None:
        bins = self._bins
        self._bins = {}
        bins_s = ", ".join(f">={dur}ms: {cnt}" for dur, cnt in bins.items())
        loop = asyncio.get_event_loop()
        loop_scheduled = len(getattr(loop, "_scheduled", None) or ())
        loop_ready = len(getattr(loop, "_ready", None) or ())
        self._logger.debug(
            "Latency stats: %s, len(loop._ready)=%s, len(loop._scheduled)=%s",
            bins_s,
            loop_ready,
            loop_scheduled,
            extra=dict(
                latency_stats=bins,
                loop_ready=loop_ready,
                loop_scheduled=loop_scheduled,
            ),
        )

    def _maybe_log_stats(self) -> None:
        now = time.monotonic()
        if now - self._last_log_time < self.stats_log_interval_sec:
            return
        self._last_log_time = now
        self._log_stats()

    def _log_warn(self, duration_sec: float) -> None:
        # The idea is to add a log line explicitly,
        # to look for logs before it,
        # to maybe find whatever caused the latency.
        self._logger.warning("High latency: %.3fs", duration_sec)

    def _handle_duration(self, duration_sec: float) -> None:
        self._log_duration(duration_sec)
        self._maybe_log_stats()
        if duration_sec >= self.min_warn_interval_sec:
            self._log_warn(duration_sec)

    async def _task_main(self) -> None:
        while True:
            ts0 = time.monotonic()
            await asyncio.sleep(self.sleep_interval_sec)
            ts1 = time.monotonic()
            self._handle_duration(ts1 - ts0 - self.sleep_interval_sec)

    async def run_task(self) -> None:
        # Does not have to be `async`, but it is more consistent this way.
        asyncio.create_task(self._task_main())


async def atest_latency_tracker() -> None:
    # Not a unit test because this is long and non-checking.
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s: %(levelname)-13s: %(name)s: %(message)s",
    )
    tracker = LatencyTracker()
    await tracker.run_task()

    part_one_duration_sec = 7.0
    LOGGER.debug("Sleeping in small steps for %rs", part_one_duration_sec)
    part_one_end = time.monotonic() + part_one_duration_sec
    while time.monotonic() < part_one_end:
        await asyncio.sleep(0.001)
        time.sleep(0.005)

    for idx in range(1, 3000, 50):
        await asyncio.sleep(0.001)
        LOGGER.debug("Sleeping for %r msec", idx)
        time.sleep(idx / 1000)


def test_latency_tracker() -> None:
    return asyncio.run(atest_latency_tracker())


if __name__ == "__main__":
    test_latency_tracker()
