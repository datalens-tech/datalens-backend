from __future__ import annotations

from collections.abc import Sequence
from typing import TypedDict

import attr

from dl_constants.enums import OperationsMode


class SchemaKWArgs(TypedDict):
    only: Sequence[str] | None
    partial: Sequence[str] | bool
    exclude: Sequence[str]
    load_only: Sequence[str]
    dump_only: Sequence[str]


@attr.s(frozen=True, auto_attribs=True)
class FieldExtra:
    partial_in: Sequence[OperationsMode] = ()
    exclude_in: Sequence[OperationsMode] = ()
    editable: bool | Sequence[OperationsMode] = ()
    export_fake: bool | None = False
