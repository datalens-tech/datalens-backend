from collections.abc import Sequence
from typing import Self

import temporalio.workflow

import dl_settings
import dl_temporal.metrics.common as common

# prometheus_client imports urllib.request, which is restricted inside the workflow sandbox.
with temporalio.workflow.unsafe.imports_passed_through():
    import dl_prometheus


class TemporalWorkflowExecutionTotalSettings(dl_settings.BaseSettings):
    pass


class TemporalWorkflowExecutionTotal(dl_prometheus.Counter):
    @classmethod
    def from_settings(cls, settings: TemporalWorkflowExecutionTotalSettings) -> Self:
        return cls(
            name="temporal_workflow_execution_total",
            documentation="Total number of Temporal workflow executions, partitioned by name and status",
            labelnames=("name", "status"),
        )

    def record(self, *, name: str, status: common.TemporalExecutionStatus) -> None:
        self.inc(labels={"name": name, "status": status.value})


class TemporalWorkflowExecutionDurationSecondsSettings(dl_settings.BaseSettings):
    BUCKETS: Sequence[float] = common.DURATION_SECONDS_DEFAULT_BUCKETS


class TemporalWorkflowExecutionDurationSeconds(dl_prometheus.Histogram):
    @classmethod
    def from_settings(cls, settings: TemporalWorkflowExecutionDurationSecondsSettings) -> Self:
        return cls(
            name="temporal_workflow_execution_duration_seconds",
            documentation="Temporal workflow execution duration in seconds, partitioned by name and status",
            labelnames=("name", "status"),
            buckets=settings.BUCKETS,
        )

    def record(self, *, name: str, status: common.TemporalExecutionStatus, duration: float) -> None:
        self.observe(duration, labels={"name": name, "status": status.value})
