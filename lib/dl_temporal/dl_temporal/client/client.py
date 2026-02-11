import asyncio
import datetime
import logging
from typing import Mapping

import attrs
import temporalio.api
import temporalio.client
import temporalio.service
from typing_extensions import Self

import dl_settings
import dl_temporal.base as base
import dl_temporal.client.exc as exc
import dl_temporal.client.metadata as metadata


LOGGER = logging.getLogger(__name__)


class TemporalClientSettings(dl_settings.BaseSettings):
    HOST: str
    PORT: int = 7233
    TLS: bool = True
    NAMESPACE: str
    METADATA_PROVIDER: dl_settings.TypedAnnotation[metadata.MetadataProviderSettings]


@attrs.define(kw_only=True, frozen=True)
class TemporalClientDependencies:
    namespace: str
    host: str
    port: int = 7233
    tls: bool = True
    lazy: bool = True
    metadata_provider: metadata.MetadataProvider = attrs.field(factory=metadata.EmptyMetadataProvider)

    @property
    def target_host(self) -> str:
        return f"{self.host}:{self.port}"


@attrs.define(kw_only=True)
class TemporalClient:
    base_client: temporalio.client.Client
    metadata_provider: metadata.MetadataProvider
    check_health_timeout: datetime.timedelta = attrs.field(default=datetime.timedelta(seconds=5))

    _update_metadata_task: asyncio.Task = attrs.field(init=False)

    @classmethod
    async def from_dependencies(cls, dependencies: TemporalClientDependencies) -> Self:
        metadata_provider = dependencies.metadata_provider
        rpc_metadata = await metadata_provider.get_metadata()

        temporal_client = await temporalio.client.Client.connect(
            target_host=dependencies.target_host,
            namespace=dependencies.namespace,
            lazy=dependencies.lazy,
            tls=dependencies.tls,
            rpc_metadata=rpc_metadata,
            data_converter=base.DataConverter(),
        )

        return cls(
            base_client=temporal_client,
            metadata_provider=metadata_provider,
        )

    async def _update_metadata(self) -> None:
        if self.metadata_provider.ttl is None:
            LOGGER.debug("Metadata TTL is None, skipping metadata update task")
            return

        while True:
            try:
                LOGGER.debug("Updating temporal client metadata")
                rpc_metadata = await self.metadata_provider.get_metadata()
                self.base_client.rpc_metadata = rpc_metadata
                await asyncio.sleep(self.metadata_provider.ttl.total_seconds())
            except Exception:
                LOGGER.exception("Error updating temporal client metadata")
                await asyncio.sleep(self.metadata_provider.error_retry_delay.total_seconds())

    def __attrs_post_init__(self) -> None:
        self._update_metadata_task = asyncio.create_task(self._update_metadata())

    async def close(self) -> None:
        if self._update_metadata_task.done():
            return
        self._update_metadata_task.cancel()
        try:
            await self._update_metadata_task
        except asyncio.CancelledError:
            pass

    async def check_health(self) -> bool:
        try:
            return await self.base_client.service_client.check_health(
                timeout=self.check_health_timeout,
            )
        except Exception:
            LOGGER.exception("Temporal client health check failed")
            return False

    async def check_auth(self) -> bool:
        try:
            # Can be replaced with any other RPC call that requires auth
            await self.base_client.workflow_service.describe_namespace(
                req=temporalio.api.workflowservice.v1.DescribeNamespaceRequest(
                    namespace=self.base_client.namespace,
                ),
            )
        except temporalio.service.RPCError as e:
            try:
                exc.wrap_temporal_error(e)
            except exc.PermissionDenied:
                return False

        return True

    async def register_namespace(
        self,
        namespace: str,
        workflow_execution_retention_period: datetime.timedelta,
    ) -> None:
        period: dict[str, int] = {"seconds": int(workflow_execution_retention_period.total_seconds())}
        try:
            await self.base_client.workflow_service.register_namespace(
                req=temporalio.api.workflowservice.v1.RegisterNamespaceRequest(
                    namespace=namespace,
                    workflow_execution_retention_period=period,  # type: ignore # so we don't need to import protobuf
                ),
            )
        except temporalio.service.RPCError as e:
            exc.wrap_temporal_error(e)

    async def add_search_attributes(
        self,
        search_attributes: Mapping[str, temporalio.api.enums.v1.IndexedValueType.ValueType],
    ) -> None:
        await self.base_client.operator_service.add_search_attributes(
            temporalio.api.operatorservice.v1.AddSearchAttributesRequest(
                namespace=self.base_client.namespace,
                search_attributes=search_attributes,
            ),
        )

    async def start_workflow(
        self,
        workflow: type[base.WorkflowProtocol[base.SelfType, base.WorkflowParamsT, base.WorkflowResultT]],
        params: base.WorkflowParamsT,
        id: str,
        task_queue: str,
    ) -> temporalio.client.WorkflowHandle[base.SelfType, base.WorkflowResultT]:
        return await self.base_client.start_workflow(
            workflow=workflow.name,
            arg=params,
            id=id,
            task_queue=task_queue,
            result_type=workflow.Result,
            execution_timeout=params.execution_timeout,
        )

    async def create_schedule(
        self,
        schedule_id: str,
        workflow: type[base.WorkflowProtocol[base.SelfType, base.WorkflowParamsT, base.WorkflowResultT]],
        params: base.WorkflowParamsT,
        task_queue: str,
        spec: temporalio.client.ScheduleSpec,
        workflow_id: str | None = None,
    ) -> temporalio.client.ScheduleHandle:
        action = temporalio.client.ScheduleActionStartWorkflow(
            workflow=workflow.name,
            arg=params,
            id=workflow_id or schedule_id,
            task_queue=task_queue,
            execution_timeout=params.execution_timeout,
        )
        schedule = temporalio.client.Schedule(
            action=action,
            spec=spec,
        )
        return await self.base_client.create_schedule(
            id=schedule_id,
            schedule=schedule,
        )

    async def update_schedule_spec(
        self,
        schedule_id: str,
        spec: temporalio.client.ScheduleSpec,
    ) -> None:
        handle = self.base_client.get_schedule_handle(id=schedule_id)

        async def _update_schedule_spec(
            input: temporalio.client.ScheduleUpdateInput,
        ) -> temporalio.client.ScheduleUpdate:
            schedule = input.description.schedule
            schedule.spec = spec
            return temporalio.client.ScheduleUpdate(schedule=input.description.schedule)

        await handle.update(_update_schedule_spec)

    async def get_schedule(
        self,
        schedule_id: str,
    ) -> temporalio.client.ScheduleHandle:
        return self.base_client.get_schedule_handle(id=schedule_id)

    async def delete_schedule(
        self,
        schedule_id: str,
    ) -> None:
        handle = self.base_client.get_schedule_handle(id=schedule_id)
        await handle.delete()

    async def pause_schedule(
        self,
        schedule_id: str,
    ) -> None:
        handle = self.base_client.get_schedule_handle(id=schedule_id)
        await handle.pause()

    async def unpause_schedule(
        self,
        schedule_id: str,
    ) -> None:
        handle = self.base_client.get_schedule_handle(id=schedule_id)
        await handle.unpause()
