from __future__ import annotations

import base64
from typing import (
    Any,
    Iterable,
    Mapping,
    Optional,
)

from marshmallow import fields
import marshmallow.utils as ma_utils


class FrozenSetField(fields.List):
    def _deserialize(self, value: Any, attr: Optional[str], data: Optional[Mapping[str, Any]], **kwargs: Any) -> frozenset[Any]:  # type: ignore  # 2024-01-30 # TODO: Return type "frozenset[Any]" of "_deserialize" incompatible with return type "list[Any]" in supertype "List"  [override]
        return frozenset(super()._deserialize(value, attr, data, **kwargs))

    def _serialize(self, value: Any, attr: Optional[str], obj: Any, **kwargs: Any) -> Optional[list[Any]]:
        do_sort = self.metadata.get("sort_output", False)
        if do_sort and value is not None:
            value = sorted(value)
        return super()._serialize(value, attr, obj, **kwargs)


class ErrorCodeField(fields.Field):
    def __init__(self, *args: Any, prefix: Optional[Iterable[str]] = None, **kwargs: Any):
        self._prefix = list(prefix or [])
        super().__init__(*args, **kwargs)

    def _serialize(self, value: Any, attr: Optional[str], obj: Any, **kwargs: Any) -> str:
        return ".".join(self._prefix + list(value))

    def _deserialize(
        self, value: Any, attr: Optional[str], data: Optional[Mapping[str, Any]], **kwargs: Any
    ) -> list[str]:
        parts = value.split(".")
        if parts[: len(self._prefix)] == self._prefix:
            parts = parts[len(self._prefix) :]
        return parts


class OnOffField(fields.Field):
    ON = "on"
    OFF = "off"

    default_error_messages = {"invalid": "Not a valid boolean."}

    def _serialize(self, value: Any, attr: Optional[str], obj: Any, **kwargs: Any) -> str:
        if value:
            return self.ON

        return self.OFF

    def _deserialize(self, value: Any, attr: Optional[str], data: Optional[Mapping[str, Any]], **kwargs: Any) -> bool:
        if value == self.ON:
            return True
        if value == self.OFF:
            return False

        raise self.make_error("invalid", input=value)


class Base64StringField(fields.Field):
    default_error_messages = {
        "invalid": "Not a valid string.",
        "invalid_utf8": "Not a valid utf-8 string.",
        "invalid_format": "Not a valid file data string",
    }

    def _deserialize(
        self, value: Any, attr: Optional[str], data: Optional[Mapping[str, Any]], **kwargs: Any
    ) -> str | None:
        if isinstance(value, bytes):
            value = value.decode(encoding="utf-8")

        if not isinstance(value, str):
            raise self.make_error("invalid")

        if value == "":
            return None

        # stripping metadata
        try:
            value = value.split(";base64,", 1)[1]
        except IndexError as exc:
            raise self.make_error("invalid_format") from exc

        value = base64.b64decode(value)
        try:
            return ma_utils.ensure_text_type(value)
        except UnicodeDecodeError as error:
            raise self.make_error("invalid_utf8") from error
