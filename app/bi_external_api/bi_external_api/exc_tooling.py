from __future__ import annotations

import abc
import contextlib
import logging
from typing import Any, Type, TypeVar, Generic, Optional, Iterator, Literal

import attr

LOGGER = logging.getLogger(__name__)

_EXC_COMPOSER_TV = TypeVar("_EXC_COMPOSER_TV", bound="ExcComposer")
_EXC_EXTRA_CONTEXT_TV = TypeVar("_EXC_EXTRA_CONTEXT_TV")


@attr.s()
class ExcComposer(Generic[_EXC_EXTRA_CONTEXT_TV], metaclass=abc.ABCMeta):
    _debug_mode: bool = attr.ib(default=False)
    _enter_occurred: bool = attr.ib(init=False, default=False)
    _is_armed: bool = attr.ib(init=False, default=False)
    _do_add_exc_message: bool = attr.ib(default=True)

    def __enter__(self: _EXC_COMPOSER_TV) -> _EXC_COMPOSER_TV:
        assert not self._enter_occurred
        self._enter_occurred = True
        return self

    def __exit__(
            self,
            exc_type: Optional[Type[Exception]],
            exc_val: Optional[Exception],
            exc_tb: Any
    ) -> Literal[False]:
        reduced_exc: Optional[Exception] = None

        # Processing unexpected exception
        if exc_val is not None:
            LOGGER.warning("Got exception in exception composer main CM", exc_info=True)
            self.handle_unexpected_exc(exc_val)

        # If we have postponed exceptions or unexpected exception fires
        # We reducing all accumulated info into single exception
        if self._is_armed or exc_val is not None:
            reduced_exc = self.reduce_exc()

        # Raise original exception in debug mode (to see original stacktrace in tests)
        if self._debug_mode:
            return False

        # If reduced exception was created we should raise it
        if reduced_exc is not None:
            if exc_val is None:
                raise reduced_exc from exc_val
            raise reduced_exc

        return False

    @property
    def is_opened(self) -> bool:
        return self._enter_occurred

    def is_armed(self) -> bool:
        return self._is_armed

    def set_armed(self) -> None:
        assert self._enter_occurred
        self._is_armed = True

    @contextlib.contextmanager
    def postponed_error(self, extra: _EXC_EXTRA_CONTEXT_TV) -> Iterator[None]:
        assert self._enter_occurred

        try:
            yield
        except Exception as exception:  # noqa
            LOGGER.warning("Got postponed exception", exc_info=True)
            self.set_armed()
            self.handle_postponed_exc(exception, extra)

            # Do no postpone errors in debug mode to get stacktrace
            if self._debug_mode:
                raise exception

    @abc.abstractmethod
    def reduce_exc(self) -> Exception:
        raise NotImplementedError()

    @abc.abstractmethod
    def handle_postponed_exc(self, exception: Exception, extra: _EXC_EXTRA_CONTEXT_TV) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def handle_unexpected_exc(self, exception: Exception) -> None:
        raise NotImplementedError()


@attr.s()
class SimpleCollectingExcComposer(ExcComposer[_EXC_EXTRA_CONTEXT_TV], metaclass=abc.ABCMeta):
    """
    Composer with implemented handle_postponed_exc()/handle_unexpected_exc() which are basically store it in self.
    Only reduce_exc() should be implemented in successors.
    """
    _postponed_errors: list[tuple[_EXC_EXTRA_CONTEXT_TV, Exception]] = attr.ib(init=False, factory=list)
    _unexpected_error: Optional[Exception] = attr.ib(init=False, default=None)

    @property
    def collected_postponed_errors(self) -> list[tuple[_EXC_EXTRA_CONTEXT_TV, Exception]]:
        return list(self._postponed_errors)

    @property
    def collected_unexpected_error(self) -> Optional[Exception]:
        return self._unexpected_error

    def handle_postponed_exc(self, exception: Exception, extra: _EXC_EXTRA_CONTEXT_TV) -> None:
        self._postponed_errors.append(
            (extra, exception,)
        )

    def handle_unexpected_exc(self, exception: Exception) -> None:
        self._unexpected_error = exception
