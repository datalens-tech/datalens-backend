from __future__ import annotations

from typing import (
    Any,
    Sequence,
)


class SingleOrMultiString:
    _value: tuple[str, ...]
    _is_single: bool

    def __init__(self, value: tuple[str, ...], is_single: bool) -> None:
        if is_single:
            assert len(value) == 1

        self._value = value
        self._is_single = is_single

    def __repr__(self) -> str:
        return f"SingleOrMultiString({repr(self._value)}, is_single={repr(self._is_single)})"

    @property
    def is_single(self) -> bool:
        return self._is_single

    @property
    def value(self) -> tuple[str, ...]:
        return self._value

    def as_single(self) -> str:
        assert self.is_single
        return self.value[0]

    def as_sequence(self) -> Sequence[str]:
        assert not self.is_single
        return self.value

    @classmethod
    def from_string(cls, s: str) -> SingleOrMultiString:
        return SingleOrMultiString((s,), is_single=True)

    @classmethod
    def from_sequence(cls, s_seq: Sequence[str]) -> SingleOrMultiString:
        return SingleOrMultiString(tuple(s_seq), is_single=False)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, SingleOrMultiString):
            return self._is_single == other._is_single and self._value == self._value
        return False

    def __hash__(self) -> int:
        return hash((self._value, self._is_single))
