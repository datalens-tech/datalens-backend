from collections.abc import (
    Hashable,
    Iterator,
    Mapping,
    Sequence,
)
from typing import (
    TypeVar,
    Union,
)


class FrozenMappingStrToStrOrStrSeq(Mapping[str, Union[str, Sequence[str]]], Hashable):
    _dict: dict[str, Union[str, Sequence[str]]]

    @staticmethod
    def ensure_tuple_of_str(seq: Sequence[str]) -> tuple[str, ...]:
        for idx, item in enumerate(seq):
            assert isinstance(item, str), f"Item {idx=} is not a string"
        return tuple(seq)

    def __init__(self, mapping: Mapping[str, Union[str, Sequence[str]]]) -> None:
        self._dict = {k: v if isinstance(v, str) else self.ensure_tuple_of_str(v) for k, v in mapping.items()}

    def __getitem__(self, k: str) -> Union[str, Sequence[str]]:
        return self._dict.__getitem__(k)

    def __len__(self) -> int:
        return len(self._dict)

    def __iter__(self) -> Iterator[str]:
        return iter(self._dict)

    def __hash__(self) -> int:
        return hash(tuple(sorted(self.items())))

    def __repr__(self) -> str:
        return f"FrozenMappingStrToStrOrStrSeq({repr(self._dict)})"


_FM_VAL_T = TypeVar("_FM_VAL_T")


class FrozenStrMapping(Mapping[str, _FM_VAL_T], Hashable):
    _dict: dict[str, _FM_VAL_T]

    def __init__(self, mapping: Mapping[str, _FM_VAL_T]) -> None:
        for k, _ in mapping.items():
            assert isinstance(k, str), f"Got non str key for FrozenStrMapping: {k!r}"

        self._dict = dict(mapping)

    def __getitem__(self, k: str) -> _FM_VAL_T:
        return self._dict.__getitem__(k)

    def __len__(self) -> int:
        return len(self._dict)

    def __iter__(self) -> Iterator[str]:
        return iter(self._dict)

    def __hash__(self) -> int:
        return hash(tuple(sorted(self.items())))

    def __repr__(self) -> str:
        return f"FrozenStrMapping({repr(self._dict)})"
