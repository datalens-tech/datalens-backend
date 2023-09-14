from __future__ import annotations

from typing import (
    Collection,
    Iterator,
    Optional,
)

import attr

from bi_core.fields import BIField


@attr.s
class FieldRegistry:
    """A collection of fields that provides easy access to them by id, title, iteration, etc."""

    _field_list: list[BIField] = attr.ib(init=False, factory=list)
    _fields_by_id: dict[str, BIField] = attr.ib(init=False, factory=dict)
    _fields_by_title: dict[str, BIField] = attr.ib(init=False, factory=dict)

    def get(self, id: Optional[str] = None, title: Optional[str] = None) -> BIField:
        if id is not None and title is not None:
            raise ValueError("Cannot specify both id and title")

        if id is not None:
            return self._fields_by_id[id]

        if title is not None:
            return self._fields_by_title[title]

        raise ValueError("Either id or title is required")

    def add(self, field: BIField) -> None:
        self._field_list.append(field)
        self._fields_by_id[field.guid] = field
        self._fields_by_title[field.title] = field

    def remove(self, field: BIField) -> None:
        self._fields_by_title.pop(field.title, None)
        self._fields_by_id.pop(field.guid, None)
        self._field_list.remove(field)

    @property
    def titles_to_guids(self) -> dict[str, str]:
        return {title: field.guid for title, field in self._fields_by_title.items()}

    @property
    def titles(self) -> Collection[str]:
        return set(self._fields_by_title.keys())

    @property
    def ids(self) -> Collection[str]:
        return set(self._fields_by_id.keys())

    def __iter__(self) -> Iterator[BIField]:
        return iter(self._field_list)
