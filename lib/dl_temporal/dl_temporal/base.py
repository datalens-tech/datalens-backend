import abc
import dataclasses
import enum
import functools
import logging
from typing import (
    Any,
    Awaitable,
    Callable,
    ClassVar,
    Generic,
    Protocol,
    TypeVar,
    cast,
)

import pydantic
import temporalio.activity
import temporalio.api.common.v1
import temporalio.common
import temporalio.converter
import temporalio.workflow

import dl_json


with temporalio.workflow.unsafe.imports_passed_through():
    import dl_logging
    import dl_pydantic


LOGGER = logging.getLogger(__name__)


class SearchAttribute(str, enum.Enum):
    RESULT_TYPE = "ResultType"
    RESULT_CODE = "ResultCode"

    @property
    def keyword(self) -> temporalio.common.SearchAttributeKey[str]:
        return temporalio.common.SearchAttributeKey.for_keyword(self.value)


class ResultType(str, enum.Enum):
    SUCCESS = "Success"
    ERROR = "Error"


def _generate_workflow_id() -> str:
    return str(temporalio.workflow.uuid4())


class BaseModel(dl_pydantic.BaseModel):
    __pydantic_is_temporal_model__: ClassVar[bool] = True

    def model_dump_for_logging(self) -> str:
        data = self.model_dump(mode="json")
        return dl_json.dumps_str(data)


class _UnsetStr(str):
    ...


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
    # per try timeout
    start_to_close_timeout: dl_pydantic.JsonableTimedelta = dl_pydantic.JsonableTimedelta(minutes=1)
    # overall timeout
    schedule_to_close_timeout: dl_pydantic.JsonableTimedelta = dl_pydantic.JsonableTimedelta(minutes=10)
    # timeout before activity is started
    schedule_to_start_timeout: dl_pydantic.JsonableTimedelta = dl_pydantic.JsonableTimedelta(minutes=10)


class BaseActivityResult(BaseResultModel):
    ...


class BaseActivityError(BaseActivityResult):
    ...


ActivityParamsT = TypeVar("ActivityParamsT", bound=BaseActivityParams)
ActivityResultT = TypeVar("ActivityResultT", bound=BaseActivityResult)


class ActivityProtocol(Protocol[ActivityParamsT, ActivityResultT]):
    name: ClassVar[str]
    logger: ClassVar[logging.Logger]

    # After python 3.13 migrate to typing.ReadOnly[type[ActivityParamsT]]
    # and move declaration to the class body.
    Params: ClassVar[type[ActivityParamsT]]  # type: ignore
    Result: ClassVar[type[ActivityResultT]]  # type: ignore

    async def run(self, params: ActivityParamsT) -> ActivityResultT:
        ...


_ActivityType = ActivityProtocol[ActivityParamsT, ActivityResultT]


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


def _activity_logging_middleware(
    func: Callable[[_ActivityType, ActivityParamsT], Awaitable[ActivityResultT]],
) -> Callable[[_ActivityType, ActivityParamsT], Awaitable[ActivityResultT]]:
    @functools.wraps(func)
    async def inner(
        self: _ActivityType,
        params: ActivityParamsT,
    ) -> ActivityResultT:
        logging_context = _activity_info_to_logging_context(
            activity_info=temporalio.activity.info(),
        )

        with dl_logging.LogContext(**logging_context):
            self.logger.info(
                "TemporalActivity(name=%s).run: starting with params: %s",
                self.name,
                params.model_dump_for_logging(),
            )

            try:
                result = await func(self, params)
            except Exception:
                self.logger.exception("TemporalActivity(name=%s).run: failed", self.name)
                raise

            if isinstance(result, BaseActivityError):
                self.logger.error(
                    "TemporalActivity(name=%s).run: finished with error: %s",
                    self.name,
                    result.model_dump_for_logging(),
                )
            elif isinstance(result, BaseActivityResult):
                self.logger.info(
                    "TemporalActivity(name=%s).run: completed with result: %s",
                    self.name,
                    result.model_dump_for_logging(),
                )
            else:
                self.logger.error(
                    "TemporalActivity(name=%s).run: finished with result of unexpected type: %s",
                    self.name,
                    type(result),
                )

            return result

    return inner


class BaseActivity(ActivityProtocol, Generic[ActivityParamsT, ActivityResultT]):
    name: ClassVar[str]
    logger: ClassVar[logging.Logger]

    @abc.abstractmethod
    async def run(self, params: ActivityParamsT) -> ActivityResultT:
        ...


class BaseWorkflowParams(BaseModel):
    execution_timeout: dl_pydantic.JsonableTimedelta = dl_pydantic.JsonableTimedelta(minutes=10)
    parent_close_policy: temporalio.workflow.ParentClosePolicy = temporalio.workflow.ParentClosePolicy.TERMINATE


class BaseWorkflowResult(BaseResultModel):
    @property
    def search_attributes(self) -> list[temporalio.common.SearchAttributeUpdate]:
        return []


class BaseWorkflowError(BaseWorkflowResult):
    ...


WorkflowParamsT = TypeVar("WorkflowParamsT", bound=BaseWorkflowParams)
WorkflowResultT = TypeVar("WorkflowResultT", bound=BaseWorkflowResult)
SelfType = TypeVar("SelfType", covariant=True)


class WorkflowProtocol(Protocol[SelfType, WorkflowParamsT, WorkflowResultT]):
    name: ClassVar[str]
    logger: ClassVar[logging.Logger]

    # After python 3.13 migrate to typing.ReadOnly[type[WorkflowParamsT]]
    # and move declaration to the class body.
    Params: ClassVar[type[WorkflowParamsT]]  # type: ignore
    Result: ClassVar[type[WorkflowResultT]]  # type: ignore

    async def run(self: SelfType, params: WorkflowParamsT) -> WorkflowResultT:
        ...


_WorkflowType = WorkflowProtocol[SelfType, WorkflowParamsT, WorkflowResultT]


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


def _workflow_logging_middleware(
    func: Callable[[_WorkflowType, WorkflowParamsT], Awaitable[WorkflowResultT]],
) -> Callable[[_WorkflowType, WorkflowParamsT], Awaitable[WorkflowResultT]]:
    @functools.wraps(func)
    async def inner(
        self: _WorkflowType,
        params: WorkflowParamsT,
    ) -> WorkflowResultT:
        logging_context = _workflow_info_to_logging_context(
            workflow_info=temporalio.workflow.info(),
        )

        with dl_logging.LogContext(**logging_context):
            self.logger.info(
                "TemporalWorkflow(name=%s).run: starting with params: %s",
                self.name,
                params.model_dump_for_logging(),
            )

            try:
                result = await func(self, params)
            except Exception:
                self.logger.exception(
                    "TemporalWorkflow(name=%s).run: failed",
                    self.name,
                )
                raise

            if isinstance(result, BaseWorkflowError):
                self.logger.error(
                    "TemporalWorkflow(name=%s).run: finished with error: %s",
                    self.name,
                    result.model_dump_for_logging(),
                )
            elif isinstance(result, BaseWorkflowResult):
                self.logger.info(
                    "TemporalWorkflow(name=%s).run: completed with result: %s",
                    self.name,
                    result.model_dump_for_logging(),
                )
            else:
                self.logger.error(
                    "TemporalWorkflow(name=%s).run: finished with result of unexpected type: %s",
                    self.name,
                    type(result),
                )

            return result

    return inner


def _search_attributes_middleware(
    func: Callable[[_WorkflowType, WorkflowParamsT], Awaitable[WorkflowResultT]],
) -> Callable[[_WorkflowType, WorkflowParamsT], Awaitable[WorkflowResultT]]:
    @functools.wraps(func)
    async def inner(
        self: _WorkflowType,
        params: WorkflowParamsT,
    ) -> WorkflowResultT:
        result = await func(self, params)
        temporalio.workflow.upsert_search_attributes(result.search_attributes)
        return result

    return inner


class BaseWorkflow(WorkflowProtocol, Generic[SelfType, WorkflowParamsT, WorkflowResultT]):
    name: ClassVar[str]
    logger: ClassVar[logging.Logger]

    # Hereby the story why we need this __init_subclass__ madness:
    # Original idea was to create def run in Base, that would use _run method in the subclass
    # and everything was fun and dandy until temporalio kicked in and started to require the run method to be defined in the subclass
    # After some research i decided to try different approaches:
    # 1. Decorators, but the decorator will have to be used explicitly in the subclass, which is not very convenient
    # 2. Leave basic run method and overwrite it in the subclass `async with def run(params): return await self.super().run(params)`
    #    but it also requires explicit override and without it the temporalio will crush with unreadable error
    # 3. Explicit metaclass, which adds override to the subclass, but it was typing mess due to Generic + Metaclass combination
    # 4. This approach, which is not very elegant, but it works and is type-safe
    def __init_subclass__(cls, **kwargs: Any) -> None:
        cls.run = _workflow_logging_middleware(cls.run)  # type: ignore
        cls.run = _search_attributes_middleware(cls.run)  # type: ignore

    @abc.abstractmethod
    async def run(self, params: WorkflowParamsT) -> WorkflowResultT:
        ...

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
            parent_close_policy=params.parent_close_policy,
            result_type=workflow.Result,
        )


def define_activity(activity: type[ActivityProtocol]) -> type[ActivityProtocol]:
    # can't use __init_subclass__ for consistency with WorkflowProtocol
    # because double wrapping will happen for some reason
    activity.run = _activity_logging_middleware(activity.run)  # type: ignore
    temporalio.activity.defn(name=activity.name)(activity.run)
    return activity


def define_workflow(workflow: type[WorkflowProtocol]) -> type[WorkflowProtocol]:
    temporalio.workflow.run(workflow.run)
    temporalio.workflow.defn(name=workflow.name)(workflow)
    return workflow
