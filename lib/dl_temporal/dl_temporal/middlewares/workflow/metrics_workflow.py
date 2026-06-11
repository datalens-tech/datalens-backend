import attrs
import temporalio.workflow

import dl_temporal.base as base
import dl_temporal.middlewares.protocol as protocol

with temporalio.workflow.unsafe.imports_passed_through():
    import dl_temporal.metrics.common as common
    import dl_temporal.metrics.workflow as workflow_metrics


@attrs.define(frozen=True, kw_only=True)
class MetricsWorkflowMiddleware:
    _execution_total: workflow_metrics.TemporalWorkflowExecutionTotal
    _duration_seconds: workflow_metrics.TemporalWorkflowExecutionDurationSeconds

    async def process(
        self,
        workflow: type[base.WorkflowProtocol],
        params: base.BaseWorkflowParams,
        handler: protocol.WorkflowHandler,
    ) -> base.BaseWorkflowResult:
        name = workflow.name
        try:
            result = await handler(params)
        except Exception:
            self._record(name=name, status=common.TemporalExecutionStatus.FAILURE)
            raise

        status = common.TemporalExecutionStatus.ERROR if result.is_error else common.TemporalExecutionStatus.SUCCESS
        self._record(name=name, status=status)
        return result

    def _record(self, *, name: str, status: common.TemporalExecutionStatus) -> None:
        # Workflow code re-runs on replay; emit only on the live execution so it counts once.
        if temporalio.workflow.unsafe.is_replaying():
            return
        duration = (temporalio.workflow.now() - temporalio.workflow.info().start_time).total_seconds()
        self._execution_total.record(name=name, status=status)
        self._duration_seconds.record(name=name, status=status, duration=duration)
