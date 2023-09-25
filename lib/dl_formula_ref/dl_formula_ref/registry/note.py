from __future__ import annotations

from enum import Enum
from typing import (
    Any,
    List,
    NamedTuple,
)

import attr

from dl_formula_ref.registry.scopes import SCOPES_DEFAULT
from dl_formula_ref.registry.text import ParameterizedText


class NoteLevel(Enum):
    info = "info"
    warning = "warning"
    alert = "alert"


class Note(NamedTuple):
    text: str
    level: NoteLevel = NoteLevel.info
    formatting: bool = True
    scopes: int = SCOPES_DEFAULT

    def format(self, *args: Any, **kwargs: Any) -> Note:
        return self._replace(text=self.text.format(*args, **kwargs))


class NoteType(Enum):
    REGULAR = "REGULAR"
    ARG_RESTRICTION = "ARG_RESTRICTION"


@attr.s(frozen=True)
class ParameterizedNote:
    param_text: ParameterizedText = attr.ib(kw_only=True)
    level: NoteLevel = attr.ib(kw_only=True, default=NoteLevel.info)
    formatting: bool = attr.ib(kw_only=True, default=True)
    type: NoteType = attr.ib(kw_only=True, default=NoteType.REGULAR)

    @property
    def text(self) -> str:
        return self.param_text.format()

    def clone(self, **kwargs: Any) -> ParameterizedNote:
        return attr.evolve(self, **kwargs)


@attr.s(frozen=True)
class CrosslinkNote(ParameterizedNote):
    ref_list: List[str] = attr.ib(kw_only=True, default=list())
