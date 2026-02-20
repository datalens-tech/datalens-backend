from enum import Enum
import functools
from itertools import islice
import operator
from typing import (
    Any,
    Iterable,
    TypeVar,
)
import uuid

import attr


def get_type_full_name(t: type) -> str:
    module = t.__module__
    qual_name = t.__qualname__

    if module == "builtins":
        return qual_name
    return f"{module}.{qual_name}"


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
        if not self.data:
            return False

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


def enum_not_none(val: _ENUM_TV | None) -> _ENUM_TV:
    assert val is not None
    return val


def make_url(
    protocol: str,
    host: str,
    port: int,
    path: str | None = None,
) -> str:
    # TODO FIX: Sanitize/use urllib
    if path is None:
        path = ""
    return f"{protocol}://{host}:{port}/{path.lstrip('/')}"


def hide_url_args(url_or_path: str) -> str:
    """Example:
    /api/v1/query?order_by=category&token=mytoken → /api/v1/query?order_by=[hidden]&token=[hidden]

    Only for logging purposes. Does not perform url validation!
    """
    if not url_or_path or "?" not in url_or_path:
        return url_or_path
    path, _, params = url_or_path.partition("?")
    fragment = "".join(params.partition("#")[1:])
    hidden_params = "&".join(f"{p.partition('=')[0]}=[hidden]" for p in params.split("&") if "=" in p)
    return f"{path}?{hidden_params}{fragment}"


def request_id_generator(prefix: str | None = None) -> str:
    result = uuid.uuid4().hex
    if prefix is not None:
        result = prefix + "." + result
    return result


def append_retry_suffix(request_id: str, attempt_number: int) -> str:
    if attempt_number <= 1:
        return request_id
    return f"{request_id}/{attempt_number}"


def make_uuid_from_parts(current: str, parent: str | None = None) -> str:
    assert current

    if not parent:
        return current

    uuid_maxlen = 120
    uuid_sep = "--"  # need to be a non-word character, to get successfully separated by elasticsearch
    cutted_replace = "..."

    if len(parent) + len(current) > uuid_maxlen:
        cutted_half_len = int((uuid_maxlen - len(current) - len(cutted_replace)) / 2)
        cutted_parent = parent[:cutted_half_len] + cutted_replace + parent[-cutted_half_len:]
        parent = cutted_parent

    result = parent + uuid_sep + current
    return result
