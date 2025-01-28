import abc
import json
from typing import (
    Any,
    Hashable,
    NamedTuple,
    get_args,
)

from dl_query_processing.postprocessing.primitives import PostprocessedValue


class HashableValuePackerBase(abc.ABC):
    """
    Base class for value packers.
    Used for packing unhashable `PostprocessedValue` values into `DataCell` objects.
    """

    @abc.abstractmethod
    def pack(self, value: PostprocessedValue) -> Hashable:
        """Turn any value into a hashable one"""

        raise NotImplementedError

    @abc.abstractmethod
    def unpack(self, hashable_value: Hashable) -> PostprocessedValue:
        """Restore original value from custom hashable"""

        raise NotImplementedError


class JsonWrapper(NamedTuple):
    """
    Simple wrapper for JSON-packed values
    to make them distinguishable from regular strings.
    """

    json: str


class JsonHashableValuePacker(HashableValuePackerBase):
    """Packs `dict`s into JSON string wrappers. Unpacks by loading JSON"""

    def pack(self, value: PostprocessedValue) -> Hashable:
        if isinstance(value, (dict, list)):
            return JsonWrapper(json=json.dumps(value, sort_keys=True))

        return value

    def unpack(self, hashable_value: Hashable) -> PostprocessedValue:
        value = hashable_value
        if isinstance(hashable_value, JsonWrapper):
            value = json.loads(hashable_value.json)

        assert isinstance(value, get_args(PostprocessedValue))
        return value


class HashableWrapper:
    """
    Wrapper that makes unhashable objects hashable.
    Contains:
    - `value` - original value;
    - `hashable` - a hashable lossless representation.

    Neither `NamedTuple` nor `attr.s` objects work well with customized hashing,
    so using just a plain slotted object.
    """

    __slots__ = ("value", "hashable")

    def __init__(self, value: Any, hashable: Hashable):
        self.value = value
        self.hashable = hashable

    def __hash__(self) -> int:
        return hash(self.hashable)

    def _norm_other_hashable(self, other: Any) -> Any:
        if isinstance(other, HashableWrapper):
            return other.hashable
        return other

    # Ordering operations for use by the pivoting logic

    def __gt__(self, other: Any) -> bool:
        return self.hashable > self._norm_other_hashable(other)

    def __ge__(self, other: Any) -> bool:
        return self.hashable >= self._norm_other_hashable(other)

    def __lt__(self, other: Any) -> bool:
        return self.hashable < self._norm_other_hashable(other)

    def __le__(self, other: Any) -> bool:
        return self.hashable <= self._norm_other_hashable(other)

    def __eq__(self, other: Any) -> bool:
        return self.hashable == self._norm_other_hashable(other)

    def __ne__(self, other: Any) -> bool:
        return self.hashable != self._norm_other_hashable(other)


class FastJsonHashableValuePacker(HashableValuePackerBase):
    """
    Packs `dict`s into wrappers containing a JSON representation
    and also the original value (for fast unpacking).
    Unpacks by returning the saved original value.
    """

    def pack(self, value: PostprocessedValue) -> Hashable:
        if isinstance(value, (dict, list)):
            return HashableWrapper(value=value, hashable=json.dumps(value, sort_keys=True))

        return value

    def unpack(self, hashable_value: Hashable) -> PostprocessedValue:
        value = hashable_value
        if isinstance(hashable_value, HashableWrapper):
            value = hashable_value.value

        assert isinstance(value, get_args(PostprocessedValue))
        return value
