from __future__ import annotations

from enum import Enum, unique
from typing import NamedTuple, Optional, Sequence, Tuple

from bi_formula.core.position import Position


@unique
class MessageLevel(Enum):
    ERROR = 'error'
    WARNING = 'warning'


class FormulaErrorCtx(NamedTuple):
    message: str
    level: MessageLevel
    position: Position = Position()
    token: Optional[str] = None
    code: Tuple[str, ...] = ()

    def is_error(self) -> bool:
        return self.level is MessageLevel.ERROR

    def is_warning(self) -> bool:
        return self.level is MessageLevel.WARNING

    def __str__(self) -> str:
        return self.message

    @property
    def coords(self) -> Tuple[Optional[int], Optional[int]]:
        """Convert position to 2-dimensional coords"""
        return self.position.start_row, self.position.start_col

    def is_sub_error(self, code: Sequence[str]) -> bool:
        if len(self.code) < len(code):
            return False
        return tuple(code) == self.code[:len(code)]
