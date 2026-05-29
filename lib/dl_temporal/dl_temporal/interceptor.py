import dataclasses
import functools
from typing import Any

import attrs
import temporalio.worker

import dl_temporal.base as base
import dl_temporal.middlewares as middlewares


class _ActivityInbound(temporalio.worker.ActivityInboundInterceptor):
    def __init__(
        self,
        next: temporalio.worker.ActivityInboundInterceptor,
        activity_middlewares: tuple[middlewares.ActivityMiddleware, ...],
    ) -> None:
        super().__init__(next)
        self._middlewares = activity_middlewares

    async def execute_activity(self, input: temporalio.worker.ExecuteActivityInput) -> Any:
        activity: base.ActivityProtocol = input.fn.__self__  # type: ignore[attr-defined]

        async def call_next(params: base.BaseActivityParams) -> base.BaseActivityResult:
            return await self.next.execute_activity(dataclasses.replace(input, args=(params,)))

        async def dispatch(
            index: int,
            params: base.BaseActivityParams,
        ) -> base.BaseActivityResult:
            if index >= len(self._middlewares):
                return await call_next(params)
            return await self._middlewares[index].process(
                activity,
                params,
                functools.partial(dispatch, index + 1),
            )

        return await dispatch(0, input.args[0])


@attrs.define(frozen=True, kw_only=True)
class TemporalInterceptor(temporalio.worker.Interceptor):
    workflow_middlewares: tuple[middlewares.WorkflowMiddleware, ...]
    activity_middlewares: tuple[middlewares.ActivityMiddleware, ...]

    def intercept_activity(
        self,
        next: temporalio.worker.ActivityInboundInterceptor,
    ) -> temporalio.worker.ActivityInboundInterceptor:
        return _ActivityInbound(next, self.activity_middlewares)

    def workflow_interceptor_class(
        self,
        input: temporalio.worker.WorkflowInterceptorClassInput,
    ) -> type[temporalio.worker.WorkflowInboundInterceptor] | None:
        captured_middlewares = self.workflow_middlewares

        class _WorkflowInbound(temporalio.worker.WorkflowInboundInterceptor):
            async def execute_workflow(self, input: temporalio.worker.ExecuteWorkflowInput) -> Any:
                workflow: type[base.WorkflowProtocol] = input.type

                async def call_next(params: base.BaseWorkflowParams) -> base.BaseWorkflowResult:
                    return await self.next.execute_workflow(dataclasses.replace(input, args=(params,)))

                async def dispatch(
                    index: int,
                    params: base.BaseWorkflowParams,
                ) -> base.BaseWorkflowResult:
                    if index >= len(captured_middlewares):
                        return await call_next(params)
                    return await captured_middlewares[index].process(
                        workflow,
                        params,
                        functools.partial(dispatch, index + 1),
                    )

                return await dispatch(0, input.args[0])

        return _WorkflowInbound
