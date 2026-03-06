import abc
from typing import Any


class Source(abc.ABC):
    @abc.abstractmethod
    async def fetch(self) -> Any:
        pass

    @abc.abstractmethod
    async def store(self, value: Any) -> None:
        pass

    @abc.abstractmethod
    async def check_readiness(self) -> bool:
        pass
