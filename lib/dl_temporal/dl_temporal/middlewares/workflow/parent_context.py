import attrs
import temporalio.workflow

import dl_temporal.base as base
import dl_temporal.middlewares.protocol as protocol


@attrs.define(frozen=True, kw_only=True)
class ParentContextWorkflowMiddleware:
    async def process(
        self,
        workflow: type[base.WorkflowProtocol],
        params: base.BaseWorkflowParams,
        handler: protocol.WorkflowHandler,
    ) -> base.BaseWorkflowResult:
        if params.parent_context.request_id is None:
            params.parent_context.request_id = temporalio.workflow.info().run_id
        return await handler(params)
