import time

import attrs

import dl_temporal.base as base
import dl_temporal.metrics.activity as activity_metrics
import dl_temporal.metrics.common as common
import dl_temporal.middlewares.protocol as protocol


@attrs.define(frozen=True, kw_only=True)
class MetricsActivityMiddleware:
    _execution_total: activity_metrics.TemporalActivityExecutionTotal
    _duration_seconds: activity_metrics.TemporalActivityExecutionDurationSeconds

    async def process(
        self,
        activity: base.ActivityProtocol,
        params: base.BaseActivityParams,
        handler: protocol.ActivityHandler,
    ) -> base.BaseActivityResult:
        name = activity.name
        start = time.monotonic()
        try:
            result = await handler(params)
        except Exception:
            self._record(name=name, status=common.TemporalExecutionStatus.FAILURE, start=start)
            raise

        status = common.TemporalExecutionStatus.ERROR if result.is_error else common.TemporalExecutionStatus.SUCCESS
        self._record(name=name, status=status, start=start)
        return result

    def _record(self, *, name: str, status: common.TemporalExecutionStatus, start: float) -> None:
        self._execution_total.record(name=name, status=status)
        self._duration_seconds.record(name=name, status=status, duration=time.monotonic() - start)
