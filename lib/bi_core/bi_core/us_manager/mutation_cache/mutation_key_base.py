import abc
from typing import Any


class MutationKey(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_hash(self) -> str:
        """Return string representation of hash that will become part of cache key."""
        raise NotImplementedError()

    @abc.abstractmethod
    def get_collision_tier_breaker(self) -> Any:
        """Returns less collision-affected but serializable representation of key."""
        raise NotImplementedError()

    def __eq__(self, other: Any) -> bool:
        """Overriden comparation operator. Used in tests."""
        if other is self:
            return True
        if not isinstance(other, MutationKey):
            return False
        if self.get_hash() != other.get_hash():
            return False
        return self.get_collision_tier_breaker() == other.get_collision_tier_breaker()
