import dataclasses
import datetime
from typing import (
    Any,
    ClassVar,
    Protocol,
    TypeVar,
    cast,
)

import temporalio.activity
import temporalio.api.common.v1
import temporalio.converter
import temporalio.workflow

import dl_pydantic


class BaseModel(dl_pydantic.BaseModel):
    __pydantic_is_temporal_model__: ClassVar[bool] = True


class JSONPlainPayloadConverter(temporalio.converter.JSONPlainPayloadConverter):
    def _is_pydantic_model(self, value: Any) -> bool:
        # can't just use isinstance(value, BaseModel), because it fails for ActivityParams
        return hasattr(value, "__pydantic_is_temporal_model__") and value.__pydantic_is_temporal_model__

    def _is_pydantic_model_hint(self, type_hint: type[Any] | None) -> bool:
        # can't just use issubclass(type_hint, BaseModel), because it fails for ActivityParams
        return (
            type_hint is not None
            and hasattr(type_hint, "__pydantic_is_temporal_model__")
            and type_hint.__pydantic_is_temporal_model__
        )

    def to_payload(self, value: Any) -> temporalio.api.common.v1.Payload | None:
        if self._is_pydantic_model(value):
            value = cast(BaseModel, value).model_dump(mode="json")

        return super().to_payload(value)

    def from_payload(self, payload: temporalio.api.common.v1.Payload, type_hint: type[Any] | None = None) -> Any:
        if self._is_pydantic_model_hint(type_hint):
            return cast(BaseModel, type_hint).model_validate_json(payload.data)

        return super().from_payload(payload, type_hint)


class PayloadConverter(temporalio.converter.CompositePayloadConverter):
    default_encoding_payload_converters: tuple[temporalio.converter.EncodingPayloadConverter, ...] = (
        *temporalio.converter.DefaultPayloadConverter.default_encoding_payload_converters,
        JSONPlainPayloadConverter(),
    )

    def __init__(self) -> None:
        """Create a default payload converter."""
        super().__init__(*self.default_encoding_payload_converters)


@dataclasses.dataclass(frozen=True)
class DataConverter(temporalio.converter.DataConverter):
    payload_converter_class: type[temporalio.converter.PayloadConverter] = PayloadConverter


class BaseActivityParams(BaseModel):
    start_to_close_timeout: datetime.timedelta = datetime.timedelta(minutes=10)


class BaseActivityResult(BaseModel):
    ...


ActivityParamsT = TypeVar("ActivityParamsT", bound=BaseActivityParams)
ActivityResultT = TypeVar("ActivityResultT", bound=BaseActivityResult)


class ActivityProtocol(Protocol[ActivityParamsT, ActivityResultT]):
    name: ClassVar[str]

    # After python 3.13 migrate to typing.ReadOnly[type[ActivityParamsT]]
    # and move declaration to the class body.
    Params: ClassVar[type[ActivityParamsT]]  # type: ignore
    Result: ClassVar[type[ActivityResultT]]  # type: ignore

    async def run(self, params: ActivityParamsT) -> ActivityResultT:
        ...


class BaseActivity(ActivityProtocol):
    ...


class BaseWorkflowParams(BaseModel):
    execution_timeout: datetime.timedelta = datetime.timedelta(minutes=10)


class BaseWorkflowResult(BaseModel):
    ...


WorkflowParamsT = TypeVar("WorkflowParamsT", bound=BaseWorkflowParams)
WorkflowResultT = TypeVar("WorkflowResultT", bound=BaseWorkflowResult)
SelfType = TypeVar("SelfType", covariant=True)


class WorkflowProtocol(Protocol[SelfType, WorkflowParamsT, WorkflowResultT]):
    name: ClassVar[str]

    # After python 3.13 migrate to typing.ReadOnly[type[WorkflowParamsT]]
    # and move declaration to the class body.
    Params: ClassVar[type[WorkflowParamsT]]  # type: ignore
    Result: ClassVar[type[WorkflowResultT]]  # type: ignore

    async def run(self: SelfType, params: WorkflowParamsT) -> WorkflowResultT:
        ...


class BaseWorkflow(WorkflowProtocol):
    async def execute_activity(
        self,
        activity: type[ActivityProtocol[ActivityParamsT, ActivityResultT]],
        params: ActivityParamsT,
    ) -> ActivityResultT:
        return await temporalio.workflow.execute_activity(
            activity=activity.name,
            arg=params,
            start_to_close_timeout=params.start_to_close_timeout,
            result_type=activity.Result,
        )

    async def start_child_workflow(
        self,
        workflow: type[WorkflowProtocol[SelfType, WorkflowParamsT, WorkflowResultT]],
        params: WorkflowParamsT,
    ) -> temporalio.workflow.ChildWorkflowHandle[SelfType, WorkflowResultT]:
        return await temporalio.workflow.start_child_workflow(
            workflow=workflow.name,
            arg=params,
            execution_timeout=params.execution_timeout,
            result_type=workflow.Result,
        )


def define_activity(activity: type[ActivityProtocol]) -> type[ActivityProtocol]:
    temporalio.activity.defn(name=activity.name)(activity.run)
    return activity


def define_workflow(workflow: type[WorkflowProtocol]) -> type[WorkflowProtocol]:
    temporalio.workflow.run(workflow.run)
    temporalio.workflow.defn(name=workflow.name)(workflow)
    return workflow
