from typing import Any

import pydantic

import dl_dynconfig
import dl_pydantic


class ScheduleConfig(dl_pydantic.BaseModel):
    NAME: str
    WORKFLOW_NAME: str
    TASK_QUEUE: str
    INTERVAL: dl_pydantic.JsonableTimedelta
    PARAMS: dict[str, Any] = pydantic.Field(default_factory=dict)
    PAUSED: bool = False
    PHASE: dl_pydantic.JsonableTimedelta | None = None


class TemporalSchedulesDynConfig(dl_dynconfig.DynConfig):
    ENABLED: bool = True
    SYNC_INTERVAL: dl_pydantic.JsonableTimedelta = dl_pydantic.JsonableTimedelta(seconds=60)
    SCHEDULES: list[ScheduleConfig] = pydantic.Field(default_factory=list)
