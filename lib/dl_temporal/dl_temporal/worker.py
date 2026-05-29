import temporalio.worker

import dl_temporal.base as base
import dl_temporal.client as client
import dl_temporal.interceptor as interceptor
import dl_temporal.middlewares as middlewares


def create_worker(
    task_queue: str,
    client: client.TemporalClient,
    workflows: list[type[base.WorkflowProtocol]],
    activities: list[base.ActivityProtocol],
    workflow_middlewares: tuple[middlewares.WorkflowMiddleware, ...],
    activity_middlewares: tuple[middlewares.ActivityMiddleware, ...],
) -> temporalio.worker.Worker:
    if client.base_client.service_client.config.lazy:
        raise ValueError("Can't create worker with lazy client")

    temporal_interceptor = interceptor.TemporalInterceptor(
        workflow_middlewares=workflow_middlewares,
        activity_middlewares=activity_middlewares,
    )
    return temporalio.worker.Worker(
        task_queue=task_queue,
        client=client.base_client,
        workflows=workflows,
        activities=[activity.run for activity in activities],
        interceptors=[temporal_interceptor],
    )
