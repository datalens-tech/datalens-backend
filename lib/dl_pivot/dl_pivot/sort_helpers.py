import abc
from typing import Any


class OrderableNullValueBase(abc.ABC):
    """
    An orderable, hashable and immutable value that can be used
    instead of ``None`` when sorting something.
    """

    __slots__ = ("weight",)

    weight: int

    def __init__(self, weight: int = 0):
        super().__setattr__("weight", weight)

    def __setattr__(self, key: str, value: Any) -> None:
        raise AttributeError(f"{type(self).__name__} is immutable")

    def __hash__(self) -> int:
        return 0

    def __eq__(self, other: Any) -> bool:
        return type(other) is type(self) and other.weight == self.weight

    def __ne__(self, other: Any) -> bool:
        return type(other) is not type(self) or other.weight != self.weight

    @abc.abstractmethod
    def __gt__(self, other: Any) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def __ge__(self, other: Any) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def __lt__(self, other: Any) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def __le__(self, other: Any) -> bool:
        raise NotImplementedError()


class LeastNullValue(OrderableNullValueBase):
    """
    A null/None placeholder that is less than everything else
    except for an instance of the same class with a lower weight.

    Note that -10 < -1 < 0 < 1 < 10.
    """

    __slots__ = ()

    def __gt__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and self.weight > other.weight

    def __ge__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and self.weight >= other.weight

    def __lt__(self, other: Any) -> bool:
        return not isinstance(other, type(self)) or self.weight < other.weight

    def __le__(self, other: Any) -> bool:
        return not isinstance(other, type(self)) or self.weight <= other.weight


class GreatestNullValue(OrderableNullValueBase):
    """
    A null/None placeholder that is greater than everything else
    except for an instance of the same class with a higher weight.

    Note that -10 < -1 < 0 < 1 < 10.
    """

    __slots__ = ()

    def __gt__(self, other: Any) -> bool:
        return not isinstance(other, type(self)) or self.weight > other.weight

    def __ge__(self, other: Any) -> bool:
        return not isinstance(other, type(self)) or self.weight >= other.weight

    def __lt__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and self.weight < other.weight

    def __le__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and self.weight <= other.weight


NORMAL_LEAST_NULL_VALUE = LeastNullValue()
TOTAL_GREATEST_NULL_VALUE = GreatestNullValue(1)
TOTAL_LEAST_NULL_VALUE = LeastNullValue(-1)


class InvertedSortWrapper:
    """
    A wrapper that inverts sorting for any object by inverting its ordering methods.
    It works only when comparing to other values of the same class.

    For use when a simple reverse of the sequence is not possible,
    e.g. when the wrapped value is a part of a large object with other
    attributes that have to be sorted in ascending order.
    """

    __slots__ = ("__value",)

    def __init__(self, value: Any):
        self.__value = value

    def __eq__(self, other: Any):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        assert isinstance(other, type(self))
        return self.__value == other.__value

    def __ne__(self, other: Any):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        assert isinstance(other, type(self))
        return self.__value != other.__value

    def __gt__(self, other: Any):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        assert isinstance(other, type(self))
        return self.__value < other.__value

    def __lt__(self, other: Any):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        assert isinstance(other, type(self))
        return self.__value > other.__value

    def __ge__(self, other: Any):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        assert isinstance(other, type(self))
        return self.__value <= other.__value

    def __le__(self, other: Any):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        assert isinstance(other, type(self))
        return self.__value >= other.__value


def invert(value: Any) -> InvertedSortWrapper:
    return InvertedSortWrapper(value)
