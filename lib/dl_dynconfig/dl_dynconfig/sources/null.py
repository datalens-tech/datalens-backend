from typing import (
    Any,
    Literal,
)

from typing_extensions import Self

import dl_dynconfig.sources.base as base
import dl_settings


class NullSourceSettings(dl_settings.BaseSettings):
    TYPE: Literal["null"] = "null"


class NullSource(base.Source):
    @classmethod
    def from_settings(cls, settings: NullSourceSettings) -> Self:
        return cls()

    async def fetch(self) -> Any:
        return {}

    async def store(self, value: Any) -> None:
        pass

    async def check_readiness(self) -> bool:
        return True
