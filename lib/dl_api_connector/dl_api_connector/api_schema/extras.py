from __future__ import annotations

from typing import (
    Optional,
    Sequence,
    TypedDict,
    Union,
)

import attr

from dl_constants.enums import OperationsMode


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
    export_fake: Optional[bool] = False
