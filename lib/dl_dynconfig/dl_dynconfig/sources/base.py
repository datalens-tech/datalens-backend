import abc
from typing import Any

import pydantic

import dl_settings


class BaseSourceSettings(dl_settings.TypedBaseSettings):
    type: str = pydantic.Field(alias="TYPE")


class BaseSource(abc.ABC):
    @abc.abstractmethod
    async def fetch(self) -> Any:
        pass

    @abc.abstractmethod
    async def store(self, value: Any) -> None:
        pass

    @abc.abstractmethod
    async def check_readiness(self) -> bool:
        pass
