import attrs
import temporalio.workflow

import dl_temporal.base as base
import dl_temporal.middlewares.protocol as protocol


@attrs.define(frozen=True, kw_only=True)
class SearchAttributesWorkflowMiddleware:
    async def process(
        self,
        workflow: type[base.WorkflowProtocol],
        params: base.BaseWorkflowParams,
        handler: protocol.WorkflowHandler,
    ) -> base.BaseWorkflowResult:
        result = await handler(params)
        temporalio.workflow.upsert_search_attributes(result.search_attributes)
        return result
