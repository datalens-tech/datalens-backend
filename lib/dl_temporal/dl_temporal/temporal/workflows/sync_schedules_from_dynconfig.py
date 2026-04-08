import logging

import temporalio.workflow

import dl_temporal.base as base


with temporalio.workflow.unsafe.imports_passed_through():
    import dl_temporal.temporal.activities as temporal_activities


LOGGER = logging.getLogger(__name__)


class SyncSchedulesFromDynconfigWorkflowParams(base.BaseWorkflowParams):
    ...


class SyncSchedulesFromDynconfigWorkflowResult(base.BaseWorkflowResult):
    ...


@base.define_workflow
class SyncSchedulesFromDynconfigWorkflow(base.BaseWorkflow):
    name = "sync_schedules_from_dynconfig"
    logger = LOGGER
    Params = SyncSchedulesFromDynconfigWorkflowParams
    Result = SyncSchedulesFromDynconfigWorkflowResult

    async def run(self, params: SyncSchedulesFromDynconfigWorkflowParams) -> SyncSchedulesFromDynconfigWorkflowResult:
        await self.execute_activity(
            temporal_activities.SyncSchedulesFromDynconfigActivity,
            temporal_activities.SyncSchedulesFromDynconfigActivityParams(),
        )
        return SyncSchedulesFromDynconfigWorkflowResult()
