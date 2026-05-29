from typing import Any

import attrs
import temporalio.workflow

with temporalio.workflow.unsafe.imports_passed_through():
    import dl_logging

import dl_temporal.base as base
import dl_temporal.middlewares.protocol as protocol


def _workflow_info_to_logging_context(
    workflow_info: temporalio.workflow.Info,
) -> dict[str, Any]:
    return {
        "temporal.workflow_type": workflow_info.workflow_type,
        "temporal.workflow_id": workflow_info.workflow_id,
        "temporal.workflow_run_id": workflow_info.run_id,
        "temporal.workflow_full_id": f"{workflow_info.workflow_id}.{workflow_info.run_id}",
        "temporal.workflow_namespace": workflow_info.namespace,
        "temporal.workflow_task_queue": workflow_info.task_queue,
    }


@attrs.define(frozen=True, kw_only=True)
class LoggingWorkflowMiddleware:
    async def process(
        self,
        workflow: type[base.WorkflowProtocol],
        params: base.BaseWorkflowParams,
        handler: protocol.WorkflowHandler,
    ) -> base.BaseWorkflowResult:
        logger = workflow.logger
        name = workflow.name

        logging_context = _workflow_info_to_logging_context(workflow_info=temporalio.workflow.info())
        logging_context["parent_request_id"] = params.parent_context.request_id

        with dl_logging.LogContext(context=logging_context):
            logger.info(
                "TemporalWorkflow(name=%s).run: starting with params: %s",
                name,
                params.model_dump_for_logging(),
            )
            try:
                result = await handler(params)
            except Exception:
                logger.exception("TemporalWorkflow(name=%s).run: failed", name)
                raise

            if result.is_error:
                logger.error(
                    "TemporalWorkflow(name=%s).run: finished with error: %s",
                    name,
                    result.model_dump_for_logging(),
                )
            else:
                logger.info(
                    "TemporalWorkflow(name=%s).run: completed with result: %s",
                    name,
                    result.model_dump_for_logging(),
                )

            return result
