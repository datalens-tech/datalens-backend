import datetime
import logging
from typing import (
    ClassVar,
    Sequence,
)

import attrs
import temporalio.client
import temporalio.service

import dl_temporal.base as base
import dl_temporal.client as client
from dl_temporal.schedule.config import TemporalSchedulesDynConfig
import dl_temporal.temporal.workflows as temporal_workflows


LOGGER = logging.getLogger(__name__)


@attrs.define(frozen=True, kw_only=True)
class ScheduleSyncService:
    temporal_client: client.TemporalClient
    config: TemporalSchedulesDynConfig
    task_queue: str
    workflows: Sequence[type[base.WorkflowProtocol]]

    SCHEDULE_PREFIX: ClassVar[str] = "system."

    def _resolve_workflow(self, name: str) -> type[base.WorkflowProtocol]:
        for workflow in self.workflows:
            if workflow.name == name:
                return workflow
        raise ValueError(f"Workflow '{name}' not found in provided workflows list")

    async def sync(self) -> None:
        await self.config.model_fetch()

        sync_schedules_schedule_id = f"system.{temporal_workflows.SyncSchedulesFromDynconfigWorkflow.name}"
        await self._upsert_schedule(
            schedule_id=sync_schedules_schedule_id,
            workflow=temporal_workflows.SyncSchedulesFromDynconfigWorkflow,
            params=temporal_workflows.SyncSchedulesFromDynconfigWorkflowParams(),
            task_queue=self.task_queue,
            interval=self.config.SYNC_INTERVAL,
        )

        if not self.config.ENABLED:
            LOGGER.info("Schedules are disabled, skipping sync")
            return

        expected_ids: set[str] = {f"system.{s.NAME}" for s in self.config.SCHEDULES}
        expected_ids.add(sync_schedules_schedule_id)

        for schedule in self.config.SCHEDULES:
            workflow_cls = self._resolve_workflow(schedule.WORKFLOW_NAME)
            params = workflow_cls.Params.model_validate(schedule.PARAMS)
            await self._upsert_schedule(
                schedule_id=f"system.{schedule.NAME}",
                workflow=workflow_cls,
                params=params,
                task_queue=schedule.TASK_QUEUE,
                interval=schedule.INTERVAL,
                phase=schedule.PHASE,
                paused=schedule.PAUSED,
            )

        async for entry in await self.temporal_client.list_schedules():
            if entry.id.startswith(self.SCHEDULE_PREFIX) and entry.id not in expected_ids:
                LOGGER.info("Deleting stale schedule: %s", entry.id)
                try:
                    await self.temporal_client.delete_schedule(entry.id)
                except client.NotFound:
                    LOGGER.debug("Stale schedule %s was already deleted", entry.id)

    async def _upsert_schedule(
        self,
        schedule_id: str,
        workflow: type[base.WorkflowProtocol],
        params: base.BaseWorkflowParams,
        task_queue: str,
        interval: datetime.timedelta,
        phase: datetime.timedelta | None = None,
        paused: bool = False,
    ) -> None:
        spec = temporalio.client.ScheduleSpec(
            intervals=[temporalio.client.ScheduleIntervalSpec(every=interval, offset=phase)]
        )
        handle = self.temporal_client.base_client.get_schedule_handle(schedule_id)
        try:
            description = await handle.describe()
            action = description.schedule.action
            assert isinstance(action, temporalio.client.ScheduleActionStartWorkflow)
            changed = (
                action.workflow != workflow.name
                or action.task_queue != task_queue
                or description.schedule.spec.intervals != spec.intervals
            )
            if changed:
                LOGGER.info("Updating schedule: %s", schedule_id)
                await self.temporal_client.update_schedule(
                    schedule_id=schedule_id,
                    workflow=workflow,
                    params=params,
                    task_queue=task_queue,
                    spec=spec,
                )
            else:
                LOGGER.debug("Schedule already up to date: %s", schedule_id)

            if paused and not description.schedule.state.paused:
                LOGGER.info("Pausing schedule: %s", schedule_id)
                await handle.pause()
            elif not paused and description.schedule.state.paused:
                LOGGER.info("Unpausing schedule: %s", schedule_id)
                await handle.unpause()
        except temporalio.service.RPCError as e:
            if e.status == temporalio.service.RPCStatusCode.NOT_FOUND:
                LOGGER.info("Creating schedule: %s", schedule_id)
                await self.temporal_client.create_schedule(
                    schedule_id=schedule_id,
                    workflow=workflow,
                    params=params,
                    task_queue=task_queue,
                    spec=spec,
                )
                if paused:
                    LOGGER.info("Pausing schedule: %s", schedule_id)
                    await handle.pause()
            else:
                raise
