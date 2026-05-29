from collections.abc import Iterator
import logging
from typing import cast

import attr
import pydantic

LOGGER = logging.getLogger(__name__)


def _join(path: str, name: str) -> str:
    return f"{path}.{name}" if path else name


def _walk(
    value: object,
    path: str,
    is_secret_subtree: bool,
) -> Iterator[tuple[str, str]]:
    if value is None:
        return
    if isinstance(value, str):
        if is_secret_subtree:
            yield path, value
        return
    if isinstance(value, bytes):
        if is_secret_subtree:
            yield path, value.decode("utf-8", errors="replace")
        return
    if isinstance(value, (tuple, list)):
        for i, item in enumerate(value):
            yield from _walk(item, f"{path}[{i}]", is_secret_subtree)
        return
    if isinstance(value, (set, frozenset)):
        # set/frozenset have no stable iteration order; indices are only
        # meaningful within a single walk and shouldn't be relied on
        # across processes.
        for i, item in enumerate(value):
            yield from _walk(item, f"{path}[{i}]", is_secret_subtree)
        return
    if isinstance(value, dict):
        for key, sub_value in value.items():
            yield from _walk(sub_value, f"{path}[{key}]", is_secret_subtree)
        return
    if isinstance(value, pydantic.BaseModel):
        fields = type(value).model_fields
        for name, sub_value in value:
            yield from _walk(
                sub_value,
                _join(path, name),
                is_secret_subtree or not fields[name].repr,
            )
        return
    if attr.has(type(value)):
        attrs_value = cast(attr.AttrsInstance, value)
        members = attr.asdict(attrs_value, recurse=False, retain_collection_types=True)
        for field in attr.fields(type(attrs_value)):
            yield from _walk(
                members[field.name],
                _join(path, field.name),
                is_secret_subtree or not field.repr,
            )
        return

    if is_secret_subtree:
        LOGGER.warning(
            "get_secret_strings: skipping unsupported value at %r of type %s",
            path,
            type(value).__name__,
        )


def get_secret_strings(
    settings: pydantic.BaseModel | attr.AttrsInstance,
) -> dict[str, str]:
    """Return {dotted_path: value_str} for every string carried by a repr=False field.
    Walks pydantic submodels, attrs classes (legacy fallback subtree and pure
    attrs roots), dicts, tuples, lists, sets and frozensets. Once the walk
    enters a repr=False subtree, every str leaf inside is emitted. bytes
    leaves are decoded via utf-8 with replacement. None / non-str / non-bytes
    leaves are skipped; unsupported container types are logged (WARNING when
    inside a repr=False subtree, DEBUG otherwise).
    """
    return dict(_walk(settings, path="", is_secret_subtree=False))
