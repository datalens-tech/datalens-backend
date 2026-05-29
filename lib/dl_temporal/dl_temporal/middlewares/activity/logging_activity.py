from typing import Any

import attrs
import temporalio.activity
import temporalio.workflow

with temporalio.workflow.unsafe.imports_passed_through():
    import dl_logging

import dl_temporal.base as base
import dl_temporal.middlewares.protocol as protocol


def _activity_info_to_logging_context(
    activity_info: temporalio.activity.Info,
) -> dict[str, Any]:
    return {
        "temporal.activity_type": activity_info.activity_type,
        "temporal.activity_id": activity_info.activity_id,
        "temporal.activity_attempt": activity_info.attempt,
        "temporal.activity_full_id": f"{activity_info.workflow_id}.{activity_info.workflow_run_id}.{activity_info.activity_id}.{activity_info.attempt}",
        "temporal.workflow_type": activity_info.workflow_type,
        "temporal.workflow_id": activity_info.workflow_id,
        "temporal.workflow_run_id": activity_info.workflow_run_id,
        "temporal.workflow_full_id": f"{activity_info.workflow_id}.{activity_info.workflow_run_id}",
        "temporal.workflow_namespace": activity_info.workflow_namespace,
        "temporal.workflow_task_queue": activity_info.task_queue,
    }


@attrs.define(frozen=True, kw_only=True)
class LoggingActivityMiddleware:
    async def process(
        self,
        activity: base.ActivityProtocol,
        params: base.BaseActivityParams,
        handler: protocol.ActivityHandler,
    ) -> base.BaseActivityResult:
        logger = activity.logger
        name = activity.name

        logging_context = _activity_info_to_logging_context(activity_info=temporalio.activity.info())
        logging_context["parent_request_id"] = params.parent_context.request_id

        with dl_logging.LogContext(context=logging_context):
            logger.info(
                "TemporalActivity(name=%s).run: starting with params: %s",
                name,
                params.model_dump_for_logging(),
            )
            try:
                result = await handler(params)
            except Exception:
                logger.exception("TemporalActivity(name=%s).run: failed", name)
                raise

            if result.is_error:
                logger.error(
                    "TemporalActivity(name=%s).run: finished with error: %s",
                    name,
                    result.model_dump_for_logging(),
                )
            else:
                logger.info(
                    "TemporalActivity(name=%s).run: completed with result: %s",
                    name,
                    result.model_dump_for_logging(),
                )

            return result
