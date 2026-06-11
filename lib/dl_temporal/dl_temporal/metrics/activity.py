from collections.abc import Sequence
from typing import Self

import temporalio.workflow

import dl_settings
import dl_temporal.metrics.common as common

# prometheus_client imports urllib.request, which is restricted inside the workflow sandbox.
with temporalio.workflow.unsafe.imports_passed_through():
    import dl_prometheus


class TemporalActivityExecutionTotalSettings(dl_settings.BaseSettings):
    pass


class TemporalActivityExecutionTotal(dl_prometheus.Counter):
    @classmethod
    def from_settings(cls, settings: TemporalActivityExecutionTotalSettings) -> Self:
        return cls(
            name="temporal_activity_execution_total",
            documentation="Total number of Temporal activity executions, partitioned by name and status",
            labelnames=("name", "status"),
        )

    def record(self, *, name: str, status: common.TemporalExecutionStatus) -> None:
        self.inc(labels={"name": name, "status": status.value})


class TemporalActivityExecutionDurationSecondsSettings(dl_settings.BaseSettings):
    BUCKETS: Sequence[float] = common.DURATION_SECONDS_DEFAULT_BUCKETS


class TemporalActivityExecutionDurationSeconds(dl_prometheus.Histogram):
    @classmethod
    def from_settings(cls, settings: TemporalActivityExecutionDurationSecondsSettings) -> Self:
        return cls(
            name="temporal_activity_execution_duration_seconds",
            documentation="Temporal activity execution duration in seconds, partitioned by name and status",
            labelnames=("name", "status"),
            buckets=settings.BUCKETS,
        )

    def record(self, *, name: str, status: common.TemporalExecutionStatus, duration: float) -> None:
        self.observe(duration, labels={"name": name, "status": status.value})
