from __future__ import annotations

import functools
import inspect
import itertools as it
import logging
import time
import traceback
from contextvars import ContextVar
from typing import (
    Any, Awaitable, Callable, Dict, Iterable, List, Optional, Tuple, Type, TypeVar, Union, cast,
)

import attr
import opentracing

from bi_utils.utils import get_type_full_name


LOGGER = logging.getLogger(__name__)


TTraceback = Any  # ?builtins?.?traceback?
TExcInfo = Tuple[Type[Exception], Exception, TTraceback]
TLogData = Dict[str, Any]
TStageList = Union[List[str], Tuple[str, ...], str]


@attr.s
class ProfileResult:
    """
    A mutable object that gets filled on context manager exit.

    Usage example:

        with GenericProfiler("some-profiled-block") as prof_result:
           some_code
        timing = prof_result.exec_time_sec
    """
    exec_time_sec: Optional[float] = attr.ib(default=None)
    log_data: Optional[TLogData] = attr.ib(default=None)
    exc_info: Optional[TExcInfo] = attr.ib(default=None)
    exc_value: Optional[Exception] = attr.ib(default=None)


class GenericProfiler:
    EVENT_CODE = 'profiling_result'

    CTX_OUTER_STAGES: ContextVar[Tuple[str, ...]] = ContextVar('CTX_OUTER_STAGES')
    CTX_LOCAL_PROFILER_STACK: ContextVar[List[GenericProfiler]] = ContextVar('CTX_LOCAL_PROFILER_STACK')
    USE_CTX_VARS = True

    @classmethod
    def get_outer_stages(cls) -> Tuple[str, ...]:
        outer_stages = cls.CTX_OUTER_STAGES.get(None)
        if outer_stages is None:
            outer_stages = ()
            cls.CTX_OUTER_STAGES.set(outer_stages)
        return outer_stages

    @classmethod
    def get_profilers_stack(cls) -> List["GenericProfiler"]:
        profilers_stack = cls.CTX_LOCAL_PROFILER_STACK.get(None)
        if profilers_stack is None:
            profilers_stack = []
            cls.CTX_LOCAL_PROFILER_STACK.set(profilers_stack)
        return profilers_stack

    @classmethod
    def reset_outer_stages(cls, outer_stages: Optional[TStageList] = None) -> None:
        if isinstance(outer_stages, str):
            outer_stages = cls.load_stage_stack(outer_stages)
        elif outer_stages is None:
            outer_stages = tuple()
        elif isinstance(outer_stages, (list, tuple)):
            outer_stages = tuple(outer_stages)
        else:
            raise TypeError(f"Unexpected type of outer stages: {type(outer_stages)}")

        if cls.get_profilers_stack():
            raise RuntimeError("Trying to reset outer stages of profiler while profilers stack is not empty")

        cls.CTX_OUTER_STAGES.set(tuple(outer_stages))

    @classmethod
    def reset_all(cls, outer_stages: Optional[TStageList] = None) -> None:
        if cls.get_current_stages_stack():
            current_stack = cls.get_current_stages_stack_str()
            tb_limit = 5
            LOGGER.warning(
                "Attempt to reset profiling stack %s. Traceback (last %d): %s",
                current_stack,
                tb_limit,
                traceback.format_stack(limit=tb_limit),
            )

        cls.CTX_LOCAL_PROFILER_STACK.set([])
        cls.reset_outer_stages(outer_stages)

    @classmethod
    def get_current_stages_stack(cls) -> Tuple[str, ...]:
        return tuple(it.chain(
            cls.get_outer_stages(),
            (p.stage for p in cls.get_profilers_stack())
        ))

    @classmethod
    def get_current_stages_stack_str(cls) -> str:
        return cls.dump_stage_stack(cls.get_current_stages_stack())

    @staticmethod
    def dump_stage_stack(stage_stack: Iterable[str]) -> str:
        return "/".join(stage_stack)

    @staticmethod
    def load_stage_stack(stage_stack_str: str) -> Tuple[str, ...]:
        return tuple(stage_stack_str.split("/"))

    def __init__(
            self,
            stage: str,
            extra_data: Optional[dict] = None,
            logger: Optional[logging.Logger] = None,
    ):
        self.logger = LOGGER if logger is None else logger  # type: logging.Logger
        self.stage = stage
        self.extra_data = {} if extra_data is None else extra_data

        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.profile_result: Optional[ProfileResult] = None
        self._ot_span_context: Optional[opentracing.scope.Scope] = None  # Will be filled on __enter__

    def _cleanup(self) -> None:
        self.start_time = None
        self.end_time = None
        self.profile_result = None

    def __enter__(self) -> ProfileResult:
        tracer = opentracing.global_tracer()
        self._ot_span_context = tracer.start_active_span(self.stage, child_of=tracer.active_span)
        self.get_profilers_stack().append(self)

        self.start_time = time.monotonic()

        self.profile_result = ProfileResult()
        self.pre_profile()
        return self.profile_result

    def __exit__(self, exc_type: Type[Exception], exc_val: Exception, exc_tb: TTraceback) -> None:
        try:
            self.post_profile()

            self.end_time = time.monotonic()

            if exc_type is None:
                exc_info = None
            else:
                exc_info = (exc_type, exc_val, exc_tb)
                assert self._ot_span_context is not None
                span = self._ot_span_context.span
                span.set_tag(opentracing.span.tags.ERROR, True)
                span.set_tag('bi.exc_type', get_type_full_name(exc_type))

            log_data = self._save_profiling_log(exc_type, exc_info=exc_info)
            self._update_profile_result(log_data=log_data, exc_info=exc_info)
            self._cleanup()
        finally:
            # Restore profiler stack in any case
            assert self._ot_span_context is not None
            self._ot_span_context.close()
            try:
                self.get_profilers_stack().pop()
            except IndexError:
                LOGGER.warning('Attempt to restore empty profiling stack on %s', self.stage)

    def pre_profile(self) -> None:
        pass

    def post_profile(self) -> None:
        pass

    def _save_profiling_log(self, exc_type: Type[Exception], exc_info: Optional[TExcInfo] = None) -> TLogData:
        assert self.end_time is not None
        assert self.start_time is not None
        time_diff = self.end_time - self.start_time

        # HAX: strvalue in context.exc_info; to be removed later.
        exc_info_text = None
        if exc_info is not None:
            exc_type, exc_val, _ = exc_info
            exc_info_text = traceback.format_exception_only(exc_type, exc_val)[-1]

        extra = dict(
            self.extra_data,
            event_code=self.EVENT_CODE,
            stage=self.stage,
            stage_stack=list(self.get_current_stages_stack()),
            stage_stack_str=self.get_current_stages_stack_str(),
            execution_time=int(round(time_diff * 1e7)),  # TODO: drop this weird stuff
            success=exc_type is None,
            exc_type=exc_type.__name__ if exc_type else None,
            exc_info_text=exc_info_text,
            # not in action fields:
            exec_time_sec=round(time_diff, 4),
        )

        log_exc_info: Optional[TExcInfo] = None
        if exc_info is not None:
            if isinstance(exc_info, tuple) and len(exc_info) == 3:
                log_exc_info = exc_info
            else:
                LOGGER.error('Invalid exc_info passed to _save_profiling_log: %r', exc_info)

        self.logger.info(
            "Profiling results: %s in %.4fs", self.stage, time_diff,
            extra=extra,
            exc_info=log_exc_info)

        return extra

    def _update_profile_result(self, exc_info: Optional[TExcInfo] = None, log_data: Optional[TLogData] = None) -> None:
        res = self.profile_result
        assert res is not None

        if exc_info is not None:
            res.exc_info = exc_info
            res.exc_value = exc_info[1]
        if log_data is not None:
            res.log_data = log_data
            res.exec_time_sec = log_data['exec_time_sec']


_GP_FUNC_RET_TV = TypeVar('_GP_FUNC_RET_TV')
_GP_FUNC_T = Callable[..., _GP_FUNC_RET_TV]


def generic_profiler(
        stage: str, extra_data: dict = None, logger: logging.Logger = None,
) -> Callable[[_GP_FUNC_T], _GP_FUNC_T]:

    def generic_profiler_wrap(func: _GP_FUNC_T) -> _GP_FUNC_T:

        if inspect.iscoroutinefunction(func):
            raise ValueError("This decorator shouldn't be used on coroutines; use `generic_profiler_async` instead")

        @functools.wraps(func)
        def generic_profiler_wrapper(*args: Any, **kwargs: Any) -> _GP_FUNC_RET_TV:
            with GenericProfiler(stage, extra_data=extra_data, logger=logger):
                return func(*args, **kwargs)

        return cast(_GP_FUNC_T, generic_profiler_wrapper)

    return generic_profiler_wrap


_GPA_CORO_RET_TV = TypeVar('_GPA_CORO_RET_TV')
_GPA_CORO_TV = TypeVar('_GPA_CORO_TV', bound=Callable[..., Awaitable[_GPA_CORO_RET_TV]])


def generic_profiler_async(stage: str, extra_data: dict = None, logger: logging.Logger = None) -> Callable[[_GPA_CORO_TV], _GPA_CORO_TV]:

    def generic_profiler_wrap_async(coro: _GPA_CORO_TV) -> _GPA_CORO_TV:

        if not inspect.iscoroutinefunction(coro):
            raise ValueError("This decorator may only be applied to a coroutine; use `generic_profiler` instead")

        @functools.wraps(coro)
        async def generic_profiler_wrapper_async(*args: Any, **kwargs: Any) -> _GPA_CORO_RET_TV:
            with GenericProfiler(stage, extra_data=extra_data, logger=logger):
                return await coro(*args, **kwargs)

        return cast(_GPA_CORO_TV, generic_profiler_wrapper_async)

    return generic_profiler_wrap_async
