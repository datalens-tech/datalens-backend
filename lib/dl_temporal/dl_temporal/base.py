import abc
import dataclasses
import enum
import logging
from typing import (
    Any,
    ClassVar,
    Protocol,
    TypeVar,
)

import pydantic
import temporalio.activity
import temporalio.common
import temporalio.contrib.pydantic
import temporalio.converter
import temporalio.workflow

import dl_json

with temporalio.workflow.unsafe.imports_passed_through():
    import dl_pydantic


LOGGER = logging.getLogger(__name__)


class SearchAttribute(enum.StrEnum):
    RESULT_TYPE = "ResultType"
    RESULT_CODE = "ResultCode"

    @property
    def keyword(self) -> temporalio.common.SearchAttributeKey[str]:
        return temporalio.common.SearchAttributeKey.for_keyword(self.value)


class ResultType(enum.StrEnum):
    SUCCESS = "Success"
    ERROR = "Error"


def _generate_workflow_id() -> str:
    return str(temporalio.workflow.uuid4())


class LoggingExcluded:
    """Marker for `Annotated[T, LoggingExcluded]` fields to omit from logging dumps."""


class BaseModel(dl_pydantic.BaseModel):
    def model_dump_for_logging(self) -> str:
        exclude: set[str] = set()
        for name, field in type(self).model_fields.items():
            if LoggingExcluded in field.metadata:
                exclude.add(name)
        return dl_json.dumps_str(self.model_dump(mode="json", exclude=exclude))

    @pydantic.model_validator(mode="before")
    @classmethod
    def _coerce_model_instance(cls, data: Any) -> Any:
        if isinstance(data, dl_pydantic.BaseModel) and not isinstance(data, cls):
            # Temporal's workflow sandbox may produce different class objects for the same model,
            # so we need to coerce the instance to a dict to avoid type errors.
            return data.model_dump()

        return data


class ParentContext(BaseModel):
    request_id: str | None = pydantic.Field(default=None)
    user_ip: str | None = pydantic.Field(default=None)
    trace_id: str | None = pydantic.Field(default=None)


class _UnsetStr(str): ...


class BaseResultModel(BaseModel):
    type: str = pydantic.Field(default_factory=_UnsetStr)

    @pydantic.model_validator(mode="before")
    @classmethod
    def type_only_default(cls, data: Any) -> Any:
        expected_type_value = cls.__name__

        if isinstance(data, dict):
            if "type" not in data or isinstance(data["type"], _UnsetStr):
                data["type"] = expected_type_value
            elif data["type"] != expected_type_value:
                raise ValueError(f"Type must be {expected_type_value}, got {data['type']}")

        return data


@dataclasses.dataclass(frozen=True)
class DataConverter(temporalio.converter.DataConverter):
    payload_converter_class: type[temporalio.converter.PayloadConverter] = (
        temporalio.contrib.pydantic.PydanticPayloadConverter
    )


class BaseActivityParams(BaseModel):
    # per try timeout
    start_to_close_timeout: dl_pydantic.JsonableTimedelta = dl_pydantic.JsonableTimedelta(minutes=1)
    # overall timeout
    schedule_to_close_timeout: dl_pydantic.JsonableTimedelta = dl_pydantic.JsonableTimedelta(minutes=10)
    # timeout before activity is started
    schedule_to_start_timeout: dl_pydantic.JsonableTimedelta = dl_pydantic.JsonableTimedelta(minutes=10)
    parent_context: ParentContext = pydantic.Field(default_factory=ParentContext)


class BaseActivityResult(BaseResultModel):
    is_error: ClassVar[bool] = False


class BaseActivityError(BaseActivityResult):
    is_error: ClassVar[bool] = True


ActivityParamsT = TypeVar("ActivityParamsT", bound=BaseActivityParams)
ActivityResultT = TypeVar("ActivityResultT", bound=BaseActivityResult)


class ActivityProtocol(Protocol[ActivityParamsT, ActivityResultT]):
    name: ClassVar[str]
    logger: ClassVar[logging.Logger]

    # After python 3.13 migrate to typing.ReadOnly[type[ActivityParamsT]]
    # and move declaration to the class body.
    Params: ClassVar[type[ActivityParamsT]]
    Result: ClassVar[type[ActivityResultT]]

    async def run(self, params: ActivityParamsT) -> ActivityResultT: ...


class BaseActivity[ActivityParamsT: BaseActivityParams, ActivityResultT: BaseActivityResult](ActivityProtocol):
    name: ClassVar[str]
    logger: ClassVar[logging.Logger]

    @abc.abstractmethod
    async def run(self, params: ActivityParamsT) -> ActivityResultT: ...


DEFAULT_WORKFLOW_EXECUTION_TIMEOUT = dl_pydantic.JsonableTimedelta(minutes=20)
DEFAULT_WORKFLOW_RUN_TIMEOUT = dl_pydantic.JsonableTimedelta(minutes=10)


class BaseWorkflowParams(BaseModel):
    execution_timeout: dl_pydantic.JsonableTimedelta = DEFAULT_WORKFLOW_EXECUTION_TIMEOUT
    run_timeout: dl_pydantic.JsonableTimedelta | None = DEFAULT_WORKFLOW_RUN_TIMEOUT
    parent_close_policy: temporalio.workflow.ParentClosePolicy = temporalio.workflow.ParentClosePolicy.TERMINATE
    parent_context: ParentContext = pydantic.Field(default_factory=ParentContext)


class BaseWorkflowResult(BaseResultModel):
    is_error: ClassVar[bool] = False

    @property
    def search_attributes(self) -> list[temporalio.common.SearchAttributeUpdate]:
        return []


class BaseWorkflowError(BaseWorkflowResult):
    is_error: ClassVar[bool] = True


WorkflowParamsT = TypeVar("WorkflowParamsT", bound=BaseWorkflowParams)
WorkflowResultT = TypeVar("WorkflowResultT", bound=BaseWorkflowResult)
SelfType = TypeVar("SelfType", covariant=True)


class WorkflowProtocol(Protocol[SelfType, WorkflowParamsT, WorkflowResultT]):
    name: ClassVar[str]
    logger: ClassVar[logging.Logger]

    # After python 3.13 migrate to typing.ReadOnly[type[WorkflowParamsT]]
    # and move declaration to the class body.
    Params: ClassVar[type[WorkflowParamsT]]
    Result: ClassVar[type[WorkflowResultT]]

    async def run(self: SelfType, params: WorkflowParamsT) -> WorkflowResultT: ...


class BaseWorkflow[SelfType, WorkflowParamsT: BaseWorkflowParams, WorkflowResultT: BaseWorkflowResult](
    WorkflowProtocol
):
    name: ClassVar[str]
    logger: ClassVar[logging.Logger]

    @abc.abstractmethod
    async def run(self, params: WorkflowParamsT) -> WorkflowResultT: ...

    async def execute_activity(
        self,
        activity: type[ActivityProtocol[ActivityParamsT, ActivityResultT]],
        params: ActivityParamsT,
    ) -> ActivityResultT:
        return await temporalio.workflow.execute_activity(
            activity=activity.name,
            arg=params,
            start_to_close_timeout=params.start_to_close_timeout,
            schedule_to_close_timeout=params.schedule_to_close_timeout,
            schedule_to_start_timeout=params.schedule_to_start_timeout,
            result_type=activity.Result,
        )

    async def start_child_workflow(
        self,
        workflow: type[WorkflowProtocol[SelfType, WorkflowParamsT, WorkflowResultT]],
        params: WorkflowParamsT,
        workflow_id: str | None = None,
    ) -> temporalio.workflow.ChildWorkflowHandle[SelfType, WorkflowResultT]:
        workflow_id = workflow_id or _generate_workflow_id()

        LOGGER.debug("Starting child workflow %s(id=%s)", workflow.name, workflow_id)
        return await temporalio.workflow.start_child_workflow(
            workflow=workflow.name,
            arg=params,
            id=workflow_id,
            execution_timeout=params.execution_timeout,
            run_timeout=params.run_timeout,
            parent_close_policy=params.parent_close_policy,
            result_type=workflow.Result,
        )

    async def execute_child_workflow(
        self,
        workflow: type[WorkflowProtocol[SelfType, WorkflowParamsT, WorkflowResultT]],
        params: WorkflowParamsT,
        workflow_id: str | None = None,
    ) -> WorkflowResultT:
        workflow_id = workflow_id or _generate_workflow_id()

        LOGGER.debug("Executing child workflow %s(id=%s)", workflow.name, workflow_id)
        return await temporalio.workflow.execute_child_workflow(
            workflow=workflow.name,
            arg=params,
            execution_timeout=params.execution_timeout,
            run_timeout=params.run_timeout,
            parent_close_policy=params.parent_close_policy,
            result_type=workflow.Result,
        )


def define_activity(activity: type[ActivityProtocol]) -> type[ActivityProtocol]:
    temporalio.activity.defn(name=activity.name)(activity.run)
    return activity


def define_workflow(workflow: type[WorkflowProtocol]) -> type[WorkflowProtocol]:
    temporalio.workflow.run(workflow.run)
    temporalio.workflow.defn(name=workflow.name)(workflow)
    return workflow
