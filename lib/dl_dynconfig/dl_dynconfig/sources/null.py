from typing import (
    Any,
    Literal,
)

import pydantic
from typing_extensions import Self

import dl_dynconfig.sources.base as base


class NullSourceSettings(base.BaseSourceSettings):
    type: Literal["null"] = pydantic.Field(alias="TYPE", default="null")


base.BaseSourceSettings.register("null", NullSourceSettings)


class NullSource(base.BaseSource):
    @classmethod
    def from_settings(cls, settings: NullSourceSettings) -> Self:
        return cls()

    async def fetch(self) -> Any:
        return {}

    async def store(self, value: Any) -> None:
        pass

    async def check_readiness(self) -> bool:
        return True
