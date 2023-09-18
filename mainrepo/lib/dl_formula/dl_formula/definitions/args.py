from __future__ import annotations

from typing import (
    AbstractSet,
    Optional,
    Sequence,
    Set,
    Union,
)

from dl_formula.core.datatype import DataType
from dl_formula.definitions.flags import (
    ContextFlag,
    ContextFlags,
)


class ArgTypeMatcher:
    __slots__ = ()

    def match_arg_types(self, arg_types: Sequence[DataType]) -> bool:
        raise NotImplementedError

    def get_possible_arg_types_at_pos(self, pos: int, total: int) -> Set[DataType]:
        raise NotImplementedError


class ArgTypeSequence(ArgTypeMatcher):
    __slots__ = ("_exp_arg_types",)

    def __init__(self, arg_types: Sequence[Union[DataType, AbstractSet[DataType]]]):
        self._exp_arg_types = arg_types

    def match_arg_types(self, arg_types: Sequence[DataType]) -> bool:
        for expected_arg_type, real_arg_type in zip(self._exp_arg_types, arg_types):
            if isinstance(expected_arg_type, DataType):
                # a single type
                if not real_arg_type.casts_to(expected_arg_type):
                    return False
            else:  # set, frozenset
                if not any(
                    real_arg_type.casts_to(expected_arg_sub_type) for expected_arg_sub_type in expected_arg_type
                ):
                    return False
        return True

    def get_possible_arg_types_at_pos(self, pos: int, total: int) -> Set[DataType]:
        expected_arg_type = self._exp_arg_types[pos]
        if isinstance(expected_arg_type, DataType):
            return {expected_arg_type}
        else:  # set, frozenset
            return set(expected_arg_type)


class ArgTypeForAll(ArgTypeMatcher):
    __slots__ = ("_exp_arg_types", "_require_type_match")

    def __init__(
        self,
        expected_types: Union[DataType, AbstractSet[DataType]],
        require_type_match: Optional[Union[DataType, AbstractSet[DataType]]] = None,
    ):
        self._exp_arg_types = {expected_types} if isinstance(expected_types, DataType) else expected_types
        self._require_type_match = (
            {require_type_match} if isinstance(require_type_match, DataType) else require_type_match
        )

    def match_arg_types(self, arg_types: Sequence[DataType]) -> bool:
        if self._require_type_match is not None and not any(
            real_arg_type in self._require_type_match for real_arg_type in arg_types
        ):
            return False
        for real_arg_type in arg_types:
            if not any(real_arg_type.casts_to(expected_arg_sub_type) for expected_arg_sub_type in self._exp_arg_types):
                return False
        return True

    def get_possible_arg_types_at_pos(self, pos: int, total: int) -> Set[DataType]:
        return set(self._exp_arg_types)


class ArgFlagDispenser:
    """Defines which arguments get marked with context flags"""

    __slots__ = ()

    def get_flags_for_pos(self, pos: int, total: int) -> Optional[ContextFlags]:
        raise NotImplementedError


class ArgFlagSequence(ArgFlagDispenser):
    __slots__ = ("_arg_flags",)

    def __init__(self, arg_flags: Sequence[Optional[ContextFlags]]):
        self._arg_flags = arg_flags

    def get_flags_for_pos(self, pos: int, total: int) -> Optional[ContextFlags]:
        return self._arg_flags[pos]


class IfFlagDispenser(ArgFlagDispenser):
    __slots__ = ()

    def get_flags_for_pos(self, pos: int, total: int) -> Optional[ContextFlags]:
        if pos < total - 1 and pos % 2 == 0:
            # all even positions, except for last item -> condition
            return ContextFlag.REQ_CONDITION
        return None
