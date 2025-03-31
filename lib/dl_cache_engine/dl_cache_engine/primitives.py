from __future__ import annotations

import hashlib
from typing import (
    Any,
    Hashable,
    NamedTuple,
    Optional,
)

import attr

from dl_cache_engine.exc import CacheKeyValidationError


@attr.s(frozen=True)
class CacheTTLConfig:
    ttl_sec_direct: int = attr.ib(default=60)
    ttl_sec_materialized: int = attr.ib(default=3600)  # TODO remove

    def clone(self, **kwargs: Any) -> CacheTTLConfig:
        return attr.evolve(self, **kwargs)


@attr.s(frozen=True)
class CacheTTLInfo:
    ttl_sec: int = attr.ib()
    refresh_ttl_on_read: bool = attr.ib()

    def clone(self, **kwargs: Any) -> CacheTTLInfo:
        return attr.evolve(self, **kwargs)


@attr.s(frozen=True, auto_attribs=True)
class BIQueryCacheOptions:
    cache_enabled: bool
    key: Optional[LocalKeyRepresentation]
    ttl_sec: int
    refresh_ttl_on_read: bool

    def clone(self, **kwargs: Any) -> BIQueryCacheOptions:
        return attr.evolve(self, **kwargs)


class DataKeyPart(NamedTuple):
    part_type: str
    part_content: Hashable

    def stringify(self) -> str:
        if isinstance(self.part_content, DataKeyPart):
            part_content_str = self.part_content.stringify()
        else:
            part_content_str = str(self.part_content)
        return f"{self.part_type}:{part_content_str}"

    def validate(self) -> None:
        if not self.part_type:
            raise CacheKeyValidationError("Data key part type is empty")
        if self.part_content is None:
            raise CacheKeyValidationError(f"Data key part content is None for part type {self.part_type}")


@attr.s(slots=True)
class LocalKeyRepresentation:
    _key_parts: tuple[DataKeyPart, ...] = attr.ib(default=())
    _key_parts_str: Optional[str] = attr.ib(init=False, default=None)
    _key_parts_hash: Optional[str] = attr.ib(init=False, default=None)

    def validate(self) -> None:
        if len(self.key_parts) == 0:
            raise CacheKeyValidationError("key_parts is empty")
        for key_part in self.key_parts:
            key_part.validate()

    @property
    def key_parts(self) -> tuple[DataKeyPart, ...]:
        return self._key_parts

    @property
    def key_parts_str(self) -> str:
        if self._key_parts_str is None:
            self._key_parts_str = ";".join(part.stringify() for part in self.key_parts)
        return self._key_parts_str

    @property
    def key_parts_hash(self) -> str:
        if self._key_parts_hash is None:
            self._key_parts_hash = hashlib.sha256(self.key_parts_str.encode()).hexdigest()
        return self._key_parts_hash

    def extend(self, part_type: str, part_content: Hashable) -> LocalKeyRepresentation:
        assert part_content is not None
        new_part = DataKeyPart(part_type=part_type, part_content=part_content)
        return LocalKeyRepresentation(key_parts=self.key_parts + (new_part,))

    def multi_extend(self, *parts: DataKeyPart) -> LocalKeyRepresentation:
        return LocalKeyRepresentation(key_parts=self.key_parts + parts)
