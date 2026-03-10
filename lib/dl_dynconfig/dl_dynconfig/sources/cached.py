import datetime
import logging
from typing import (
    Any,
    TypeVar,
)

import attrs

import dl_dynconfig.sources.base as base


T = TypeVar("T")
Unset = object()

LOGGER = logging.getLogger(__name__)


@attrs.define(kw_only=True)
class CachedSource(base.BaseSource):
    _source: base.BaseSource
    _ttl: datetime.timedelta = datetime.timedelta(minutes=5)

    _cached_data: Any = attrs.field(init=False, default=Unset)
    _cached_at: datetime.datetime | None = attrs.field(init=False, default=None)

    @property
    def _is_expired(self) -> bool:
        return (
            self._cached_data is Unset
            or self._cached_at is None
            or self._cached_at + self._ttl < datetime.datetime.now()
        )

    def _update_cache(self, data: T) -> T:
        self._cached_data = data
        self._cached_at = datetime.datetime.now()
        return data

    async def fetch(self) -> Any:
        if not self._is_expired:
            return self._cached_data

        data = await self._source.fetch()
        return self._update_cache(data)

    async def store(self, value: T) -> T:
        await self._source.store(value)
        return self._update_cache(value)

    async def check_readiness(self) -> bool:
        if not self._is_expired:
            return True

        return await self._source.check_readiness()
