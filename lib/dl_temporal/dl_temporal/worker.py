import temporalio.worker

import dl_temporal.base as base
import dl_temporal.client as client


def create_worker(
    task_queue: str,
    client: client.TemporalClient,
    workflows: list[type[base.WorkflowProtocol]],
    activities: list[base.ActivityProtocol],
) -> temporalio.worker.Worker:
    if client.base_client.service_client.config.lazy:
        raise ValueError("Can't create worker with lazy client")

    return temporalio.worker.Worker(
        task_queue=task_queue,
        client=client.base_client,
        workflows=workflows,
        activities=[activity.run for activity in activities],
    )
