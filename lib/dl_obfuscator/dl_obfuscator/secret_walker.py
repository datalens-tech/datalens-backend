from collections.abc import (
    Callable,
    Iterator,
)
import logging
from typing import cast

import attr
import pydantic

LOGGER = logging.getLogger(__name__)


SecretFieldResolver = Callable[[type], frozenset[str]]


def _no_extra(_cls: type) -> frozenset[str]:
    return frozenset()


def _join(path: str, name: str) -> str:
    return f"{path}.{name}" if path else name


def _walk(
    value: object,
    path: str,
    is_secret_subtree: bool,
    extra: SecretFieldResolver,
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
            yield from _walk(item, f"{path}[{i}]", is_secret_subtree, extra)
        return
    if isinstance(value, (set, frozenset)):
        # set/frozenset have no stable iteration order; indices are only
        # meaningful within a single walk and shouldn't be relied on
        # across processes.
        for i, item in enumerate(value):
            yield from _walk(item, f"{path}[{i}]", is_secret_subtree, extra)
        return
    if isinstance(value, dict):
        for key, sub_value in value.items():
            yield from _walk(sub_value, f"{path}[{key}]", is_secret_subtree, extra)
        return
    if isinstance(value, pydantic.BaseModel):
        cls = type(value)
        fields = cls.model_fields
        extra_names = extra(cls)
        for name, sub_value in value:
            yield from _walk(
                sub_value,
                _join(path, name),
                is_secret_subtree or not fields[name].repr or name in extra_names,
                extra,
            )
        return
    if attr.has(type(value)):
        attrs_value = cast(attr.AttrsInstance, value)
        attrs_cls = type(attrs_value)
        extra_names = extra(attrs_cls)
        members = attr.asdict(attrs_value, recurse=False, retain_collection_types=True)
        for field in attr.fields(attrs_cls):
            yield from _walk(
                members[field.name],
                _join(path, field.name),
                is_secret_subtree or field.repr is not True or field.name in extra_names,
                extra,
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
    extra_secret_fields: SecretFieldResolver = _no_extra,
) -> dict[str, str]:
    """Return {dotted_path: value_str} for every string carried by a secret-marked field.

    A field is secret when it is not the default repr (attrs: ``field.repr is not True``, covering
    ``repr=False`` and ``repr=<callable>`` such as ``secrepr``; pydantic: ``repr=False``) OR when its name
    is returned by ``extra_secret_fields(cls)``. Once the walk enters a secret subtree, every str leaf
    inside is emitted regardless of child repr.

    Walks pydantic submodels, attrs classes (incl. ``slots=True`` and the legacy fallback subtree on
    ``dl_settings.BaseRootSettingsWithFallback``), dicts, tuples, lists, sets and frozensets. bytes leaves
    are decoded via utf-8 with replacement. None / non-str / non-bytes leaves are skipped; unsupported
    container types inside a secret subtree are logged at WARNING.

    Pair with ``SecretKeeper.add_secrets(...)`` to bulk-register the result.
    """
    return dict(_walk(settings, path="", is_secret_subtree=False, extra=extra_secret_fields))
