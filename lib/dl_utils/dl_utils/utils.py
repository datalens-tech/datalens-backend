from __future__ import annotations

from contextlib import contextmanager
from enum import Enum
import functools
from itertools import islice
import operator
import os
import sys
from time import time
from typing import (
    Any,
    Callable,
    Generator,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    cast,
)

import attr


def get_pdb() -> Any:
    try:
        import ipdb

        return ipdb
    except Exception:
        import pdb

        return pdb


def maybe_postmortem(ei: Any = None) -> None:
    """
    An env-controlled post-mortem breakpoint.
    Set `BI_ERR_PDB=1` to use.
    """
    if ei is None:
        ei = sys.exc_info()
    if os.environ.get("BI_ERR_PDB") != "1":
        return
    _, _, sys.last_traceback = ei
    import traceback

    print("".join(traceback.format_exception(*ei)))
    pdb = get_pdb()
    pdb.pm()


_CALLABLE_TV = TypeVar("_CALLABLE_TV", bound=Callable)


def exc_catch_wrap(func: _CALLABLE_TV) -> _CALLABLE_TV:
    """
    An env-controlled post-mortem function wrapper.
    Set `BI_ERR_PDB=1` to use. Does nothing otherwise.
    """
    if os.environ.get("BI_ERR_PDB") != "1":
        return func

    @functools.wraps(func)
    def exc_catch_wrapped(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception:
            maybe_postmortem()
            raise

    return cast(_CALLABLE_TV, exc_catch_wrapped)


def exc_catch_awrap(afunc: _CALLABLE_TV) -> _CALLABLE_TV:
    """
    An env-controlled post-mortem async-function wrapper.
    Set `BI_ERR_PDB=1` to use. Does nothing otherwise.
    """
    if os.environ.get("BI_ERR_PDB") != "1":
        return afunc

    @functools.wraps(afunc)
    async def exc_catch_awrapped(*args: Any, **kwargs: Any) -> Any:
        try:
            return await afunc(*args, **kwargs)
        except Exception:
            maybe_postmortem()
            raise

    return cast(_CALLABLE_TV, exc_catch_awrapped)


def get_type_full_name(t: Type) -> str:
    module = t.__module__
    qual_name = t.__qualname__

    if module == "builtins":
        return qual_name
    return f"{module}.{qual_name}"


# split_list value TypeVar
_SL_V_TV = TypeVar("_SL_V_TV")


def split_list(
    iterable: Iterable[_SL_V_TV], condition: Callable[[_SL_V_TV], bool]
) -> Tuple[List[_SL_V_TV], List[_SL_V_TV]]:
    """
    Split list items into `(matching, non_matching)` by `condition(item)` callable.
    """
    matching: List[_SL_V_TV] = []
    non_matching: List[_SL_V_TV] = []
    for item in iterable:
        if condition(item):
            matching.append(item)
        else:
            non_matching.append(item)
    return matching, non_matching


class DotDict(dict):
    """A simple dict subclass with items also available over attributes"""

    def __getattr__(self, item: str) -> Any:
        try:
            return self[item]
        except KeyError as exc:
            raise AttributeError(*exc.args) from exc

    def __setattr__(self, key: str, value: Any) -> None:
        self[key] = value


@attr.s(frozen=True)
class DataKey:
    parts: tuple[str, ...] = attr.ib()


@attr.s
class AddressableData:
    data: dict[str, Any] = attr.ib()

    def contains(self, key: DataKey) -> bool:
        try:
            self.get(key)
            return True
        except KeyError:
            return False

    def get(self, key: DataKey) -> Any:
        return functools.reduce(operator.getitem, key.parts, self.data)

    def set(self, key: DataKey, value: Any) -> None:
        key_head = DataKey(parts=key.parts[:-1])
        self.get(key_head)[key.parts[-1]] = value

    def pop(self, key: DataKey) -> Any:
        key_head = DataKey(parts=key.parts[:-1])
        return self.get(key_head).pop(key.parts[-1])


T = TypeVar("T")


def batched(iterable: Iterable[T], n: int) -> Iterable[Iterable[T]]:
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch


def join_in_chunks(pieces: Iterable[str], sep: str, max_len: int) -> Iterable[str]:
    """An `sep.join(pieces)` equivalent that chunks the pieces to stay under the maximum piece length"""
    pieces_it = iter(pieces)
    try:
        result = next(pieces_it)
    except StopIteration:
        return  # empty pieces -> empty chunks
    for piece in pieces_it:
        full_piece = sep + piece
        if len(result) + len(full_piece) >= max_len:
            yield result
            result = piece
        else:
            result += full_piece
    yield result


_ENUM_TV = TypeVar("_ENUM_TV", bound=Enum)


def enum_not_none(val: Optional[_ENUM_TV]) -> _ENUM_TV:
    assert val is not None
    return val


def time_it(fn: Callable) -> Callable:
    @functools.wraps(fn)
    def wrap(*args: Any, **kwargs: Any) -> Any:
        t0 = time()
        print(f"Invoked {fn}({args}, {kwargs})"[:160])
        result = fn(*args, **kwargs)
        delta = time() - t0
        if delta >= 0.01:
            print(f"<< Time elapsed: {delta} for {fn}({args}, {kwargs})"[:160])
        return result

    return wrap


@contextmanager
def time_it_cm(label: str) -> Generator[None, None, None]:
    # print(f'Time it for {label}')
    t0 = time()
    yield
    delta = time() - t0
    if delta >= 0.01:
        print(f"Time elapsed for {label}: {delta}")


def make_url(
    protocol: str,
    host: str,
    port: int,
    path: Optional[str] = None,
) -> str:
    # TODO FIX: Sanitize/use urllib
    if path is None:
        path = ""
    return f"{protocol}://{host}:{port}/{path.lstrip('/')}"
