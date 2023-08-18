from __future__ import annotations

import contextlib
from typing import Sequence, Iterator, TypeVar

import attr

from bi_external_api.converter.converter_exc import CompositeConverterError, CompositeConverterErrorData
from bi_external_api.exc_tooling import ExcComposer


@attr.s()
class ConvErrCtx:
    path: Sequence[str] = attr.ib()


@attr.s()
class ConversionExcComposer(ExcComposer[ConvErrCtx]):
    _map_path_exc: dict[Sequence[str], Exception] = attr.ib(init=False, factory=dict)
    _unexpected_exc_path: Sequence[str] = attr.ib(default=())

    def set_unexpected_exc_path(self, path: Sequence[str]) -> None:
        self._unexpected_exc_path = path

    def reduce_exc(self) -> Exception:
        flatten: dict[Sequence[str], Exception] = dict()
        for path, exc in self._map_path_exc.items():
            if isinstance(exc, CompositeConverterError):
                for sub_path, sub_exc in exc.data.map_path_exc.items():
                    flatten[tuple(list(path) + list(sub_path))] = sub_exc
            else:
                flatten[path] = exc

        return CompositeConverterError(
            CompositeConverterErrorData(
                flatten
            )
        )

    def handle_postponed_exc(self, exception: Exception, extra: ConvErrCtx) -> None:
        self._map_path_exc[extra.path] = exception

    def handle_unexpected_exc(self, exception: Exception) -> None:
        self._map_path_exc[self._unexpected_exc_path] = exception


T = TypeVar("T")


@attr.s()
class ConversionErrHandlingContext:
    composer: ConversionExcComposer = attr.ib(init=False, factory=ConversionExcComposer)
    current_path: list[str] = attr.ib(factory=list[str])

    @property
    def is_opened(self) -> bool:
        return self.composer.is_opened

    def log_warning(self, message: str, not_for_user: bool) -> None:
        pass

    @contextlib.contextmanager
    def cm(self) -> Iterator[ConversionErrHandlingContext]:
        with self.composer:
            yield self

    @contextlib.contextmanager
    def push_path(self, path_elem: str) -> Iterator[None]:
        self.current_path.append(path_elem)
        try:
            yield
        # TODO CONSIDER: May be capture path of first exception just for info?
        # except Exception:
        #     pass
        finally:
            self.current_path.pop()

    @contextlib.contextmanager
    def postpone_error_with_path(self, path_elem: str) -> Iterator[None]:
        with self.push_path(path_elem):
            ctx = ConvErrCtx(tuple(self.current_path))
            with self.composer.postponed_error(ctx):
                yield

    @contextlib.contextmanager
    def postpone_error_in_current_path(self) -> Iterator[None]:
        ctx = ConvErrCtx(tuple(self.current_path))
        with self.composer.postponed_error(ctx):
            yield
