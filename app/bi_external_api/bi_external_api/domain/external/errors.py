from typing import Sequence, Optional

import attr

from bi_external_api.attrs_model_mapper import ModelDescriptor
from .common import NameMapEntry
from .workbook import WorkBook
from ..utils import ensure_tuple


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class CommonError:
    path: Optional[str] = attr.ib(default=None)
    message: str = attr.ib()
    exc_message: Optional[str] = attr.ib(default=None)
    stacktrace: Optional[str] = attr.ib(default=None)


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class EntryError:
    name: str = attr.ib()
    errors: Sequence[CommonError] = attr.ib(converter=ensure_tuple)


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class ErrWorkbookOp:
    message: str = attr.ib()
    common_errors: Sequence[CommonError] = attr.ib(converter=ensure_tuple)
    entry_errors: Sequence[EntryError] = attr.ib(converter=ensure_tuple)
    partial_workbook: Optional[WorkBook] = attr.ib(default=None)
    request_id: Optional[str] = attr.ib(default=None)


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class ErrWorkbookOpClusterization(ErrWorkbookOp):
    name_map: Optional[Sequence[NameMapEntry]] = attr.ib(converter=ensure_tuple)  # type: ignore
