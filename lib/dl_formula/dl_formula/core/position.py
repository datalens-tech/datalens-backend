from __future__ import annotations

from typing import (
    List,
    NamedTuple,
    Optional,
    Tuple,
)


class Position(NamedTuple):
    start: Optional[int] = None
    end: Optional[int] = None
    start_row: Optional[int] = None
    end_row: Optional[int] = None
    start_col: Optional[int] = None
    end_col: Optional[int] = None


class PositionConverter:
    def __init__(self, text: str):
        self._text: str = text
        self._pos_by_line: List[int] = [0]  # a list of starting positions for each line number
        for line in text.split("\n"):
            self._pos_by_line.append(self._pos_by_line[-1] + len(line) + 1)

    def idx_to_line_and_row(self, idx: int) -> Tuple[int, int]:
        cropped = self._text[:idx]
        row = cropped.count("\n")
        col = len(cropped.rsplit("\n", 1)[-1])
        return row, col

    def idx_to_position(self, idx: int):
        start = idx
        end = idx
        start_row, start_col = self.idx_to_line_and_row(idx)
        end_row, end_col = self.idx_to_line_and_row(idx)
        return Position(
            start=start,
            end=end,
            start_row=start_row,
            end_row=end_row,
            start_col=start_col,
            end_col=end_col,
        )

    def row_and_col_to_idx(self, line: int, column: int) -> int:
        return self._pos_by_line[line - 1] + column - 1

    def merge_positions(self, start_position: Position, end_position: Position):
        return Position(
            start=start_position.start,
            end=end_position.end,
            start_row=start_position.start_row,
            end_row=end_position.end_row,
            start_col=start_position.start_col,
            end_col=end_position.end_col,
        )
