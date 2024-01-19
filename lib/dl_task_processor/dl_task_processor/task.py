import abc
from collections.abc import Iterable
import enum
from typing import (
    Any,
    ClassVar,
    Generic,
    NewType,
    Optional,
    Type,
    TypeVar,
)
import uuid

import attr

from dl_task_processor.context import BaseContext


TaskName = NewType("TaskName", str)


_BASE_ID_TYPE = TypeVar("_BASE_ID_TYPE", bound="_BaseID")


@attr.s(frozen=True)
class _BaseID:
    _id: str = attr.ib()

    def to_str(self) -> str:
        return self._id

    def __str__(self) -> str:
        return self.to_str()

    @classmethod
    def make(cls: Type[_BASE_ID_TYPE]) -> _BASE_ID_TYPE:
        return cls(uuid.uuid4().hex)


@attr.s
class InstanceID(_BaseID):
    pass


@attr.s
class RunID(_BaseID):
    pass


@attr.s
class TaskInstance:
    instance_id: InstanceID = attr.ib()
    name: TaskName = attr.ib()
    params: dict = attr.ib()
    attempt: int = attr.ib(default=0)
    request_id: Optional[str] = attr.ib(default=None)


@attr.s
class BaseTaskMeta(metaclass=abc.ABCMeta):
    name: ClassVar[TaskName]

    def get_params(self, with_name: bool = False) -> dict:
        if with_name:
            return dict(
                {"name": self.name},
                **attr.asdict(self),
            )
        return attr.asdict(self)

    def make_id(self) -> str:
        params = sorted(attr.asdict(self).items())
        serialized_params = "||".join(f"{k}|{v}" for k, v in params)
        return f"{self.name}||{serialized_params}"

    # it's temporary solution
    # i'm gonna use datalens/backend/app/bi_external_api/bi_external_api/attrs_model_mapper
    # but we have to extract it to the top level
    # now we can use it only in cli
    @classmethod
    def get_schema(cls) -> Iterable[dict]:
        attr_schema = attr.fields(cls)
        return [
            {
                "name": field.name,
                "type": field.type.__name__,
            }
            for field in attr_schema
        ]


_BASE_TASK_META_TV = TypeVar("_BASE_TASK_META_TV", bound=BaseTaskMeta)
_BASE_TASK_CONTEXT_TV = TypeVar("_BASE_TASK_CONTEXT_TV", bound=BaseContext)


@attr.s(frozen=True, eq=True)
class TaskResult(metaclass=abc.ABCMeta):
    pass


class Success(TaskResult):
    pass


# system entity, you can just raise exception
class Fail(TaskResult):
    pass


@attr.s
class Retry(TaskResult):
    delay: int = attr.ib(default=60)  # seconds
    backoff: float = attr.ib(default=2.0)
    attempts: int = attr.ib(default=5)


@attr.s
class BaseExecutorTask(Generic[_BASE_TASK_META_TV, _BASE_TASK_CONTEXT_TV], metaclass=abc.ABCMeta):
    cls_meta: ClassVar[Type[_BASE_TASK_META_TV]]
    meta: _BASE_TASK_META_TV = attr.ib()
    _ctx: _BASE_TASK_CONTEXT_TV = attr.ib()
    _instance_id: InstanceID = attr.ib()
    _run_id: RunID = attr.ib()
    _request_id: Optional[str] = attr.ib(default=None)

    @classmethod
    def from_params(
        cls,
        instance_id: InstanceID,
        run_id: RunID,
        ctx: _BASE_TASK_CONTEXT_TV,
        params: dict,
        request_id: Optional[str] = None,
    ) -> "BaseExecutorTask":
        return cls(
            meta=cls.cls_meta(**params),
            ctx=ctx,
            instance_id=instance_id,
            run_id=run_id,
            request_id=request_id,
        )

    @classmethod
    def name(cls) -> TaskName:
        return cls.cls_meta.name

    @abc.abstractmethod
    async def run(self) -> TaskResult:
        pass


@attr.s
class TaskRegistry:
    _tasks: dict[TaskName, Type[BaseExecutorTask]] = attr.ib()

    @classmethod
    def create(cls, tasks: Iterable[Type[BaseExecutorTask]]) -> "TaskRegistry":
        assert sorted(
            [t.name() for t in tasks],
        ) == sorted(list(set([t.name() for t in tasks]))), "Some tasks has the same name"
        return cls(tasks={task.name(): task for task in tasks})

    def get_task(self, name: TaskName) -> Type[BaseExecutorTask]:
        return self._tasks[name]

    def get_task_meta(self, name: TaskName) -> Type[BaseTaskMeta]:
        return self.get_task(name).cls_meta

    def filter_task_meta(self, name: TaskName) -> Iterable[Type[BaseTaskMeta]]:
        return [task.cls_meta for task in self._tasks.values() if not name or name == task.name()]


class LoggerFields(enum.Enum):
    request_id = enum.auto()
    task_name = enum.auto()
    task_instance_id = enum.auto()
    task_run_id = enum.auto()
    task_params = enum.auto()
