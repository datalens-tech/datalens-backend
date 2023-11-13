from __future__ import annotations

from enum import Enum
from typing import NamedTuple

import attr

from dl_formula_ref.registry.scopes import SCOPES_DEFAULT
from dl_formula_ref.registry.text import ParameterizedText
from dl_i18n.localizer_base import Translatable


class NoteLevel(Enum):
    info = "info"
    warning = "warning"
    alert = "alert"


@attr.s(frozen=True)
class Note:
    text: Translatable = attr.ib()
    level: NoteLevel = attr.ib(kw_only=True, default=NoteLevel.info)
    formatting: bool = attr.ib(kw_only=True, default=True)
    scopes: int = attr.ib(kw_only=True, default=SCOPES_DEFAULT)


class NoteType(Enum):
    REGULAR = "REGULAR"
    ARG_RESTRICTION = "ARG_RESTRICTION"


@attr.s(frozen=True)
class ParameterizedNote:
    param_text: ParameterizedText = attr.ib(kw_only=True)
    level: NoteLevel = attr.ib(kw_only=True, default=NoteLevel.info)
    formatting: bool = attr.ib(kw_only=True, default=True)
    type: NoteType = attr.ib(kw_only=True, default=NoteType.REGULAR)
