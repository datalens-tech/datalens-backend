from __future__ import annotations

import enum
from typing import (
    Optional,
    Sequence,
    TypedDict,
    Union,
)

import attr


class OperationsMode(enum.Enum):
    pass


class CreateMode(OperationsMode):
    create = enum.auto()
    test = enum.auto()


class EditMode(OperationsMode):
    edit = enum.auto()
    test = enum.auto()


class SchemaKWArgs(TypedDict):
    only: Optional[Sequence[str]]
    partial: Union[Sequence[str], bool]
    exclude: Sequence[str]
    load_only: Sequence[str]
    dump_only: Sequence[str]


@attr.s(frozen=True, auto_attribs=True)
class FieldExtra:
    partial_in: Sequence[OperationsMode] = ()
    exclude_in: Sequence[OperationsMode] = ()
    editable: Union[bool, Sequence[OperationsMode]] = ()
