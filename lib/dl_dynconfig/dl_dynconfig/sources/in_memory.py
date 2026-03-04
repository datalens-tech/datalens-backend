import copy
from typing import Any

import attrs

import dl_dynconfig.sources.base as base


@attrs.define(kw_only=True)
class InMemorySource(base.Source):
    _data: Any

    async def fetch(self) -> Any:
        return copy.deepcopy(self._data)

    async def store(self, value: Any) -> None:
        self._data = value

    async def check_readiness(self) -> bool:
        return True
