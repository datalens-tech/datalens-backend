import temporalio.worker
import temporalio.worker.workflow_sandbox

import dl_temporal.base as base
import dl_temporal.client as client
import dl_temporal.interceptor as interceptor
import dl_temporal.middlewares as middlewares

# `frozendict` (transitively imported by dl_settings, dl_api_commons, dl_core and ~50 other
# modules) has no cp312 wheel, so it always runs its pure-Python path which monkeypatches the
# shared `collections.abc.MutableMapping.__subclasshook__` on import. The Temporal sandbox
# re-imports third-party modules for determinism while keeping stdlib shared, so each re-import
# wraps the already-patched hook again; under a workflow's retry/replay loop the delegation chain
# grows until any `isinstance(_, Mapping)` hits the recursion limit. Passing frozendict through
# the sandbox makes it import once from the host and never re-patch.
_SANDBOX_RESTRICTIONS = temporalio.worker.workflow_sandbox.SandboxRestrictions.default.with_passthrough_modules(
    "frozendict",
)


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
        workflow_runner=temporalio.worker.workflow_sandbox.SandboxedWorkflowRunner(
            restrictions=_SANDBOX_RESTRICTIONS,
        ),
    )
