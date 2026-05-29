from collections.abc import (
    Awaitable,
    Callable,
)
from typing import Protocol

import dl_temporal.base as base

ActivityHandler = Callable[[base.BaseActivityParams], Awaitable[base.BaseActivityResult]]
WorkflowHandler = Callable[[base.BaseWorkflowParams], Awaitable[base.BaseWorkflowResult]]


class ActivityMiddleware(Protocol):
    async def process(
        self,
        activity: base.ActivityProtocol,
        params: base.BaseActivityParams,
        handler: ActivityHandler,
    ) -> base.BaseActivityResult: ...


class WorkflowMiddleware(Protocol):
    async def process(
        self,
        workflow: type[base.WorkflowProtocol],
        params: base.BaseWorkflowParams,
        handler: WorkflowHandler,
    ) -> base.BaseWorkflowResult: ...
