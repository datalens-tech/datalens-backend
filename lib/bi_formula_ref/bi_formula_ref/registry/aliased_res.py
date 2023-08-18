from __future__ import annotations

from collections import ChainMap
from typing import Any, Iterable, List, Mapping, Tuple, TypeVar

import attr


_RES_TV = TypeVar('_RES_TV', bound='AliasedResource')


@attr.s(frozen=True)
class AliasedResource:
    def clone(self: _RES_TV, **kwargs: Any) -> _RES_TV:
        return attr.evolve(self, **kwargs)


@attr.s(frozen=True)
class AliasedTextResource(AliasedResource):
    body: str = attr.ib(kw_only=True)


@attr.s(frozen=True)
class AliasedLinkResource(AliasedResource):
    url: str = attr.ib(kw_only=True)


@attr.s(frozen=True)
class AliasedTableResource(AliasedResource):
    table_body: List[List[str]] = attr.ib(kw_only=True)


@attr.s(frozen=True)
class AliasedResourceRegistry:
    _resources: Mapping[str, AliasedResource] = attr.ib(factory=dict)

    def __getitem__(self, item: str) -> AliasedResource:
        return self._resources[item]

    def __add__(self, other: AliasedResourceRegistry) -> AliasedResourceRegistry:
        return AliasedResourceRegistry(resources=ChainMap(self._resources, other._resources))

    def items(self) -> Iterable[Tuple[str, AliasedResource]]:
        return self._resources.items()

    def keys(self) -> Iterable[str]:
        return self._resources.keys()

    def get_link(self, alias: str) -> AliasedLinkResource:
        res = self[alias]
        assert isinstance(res, AliasedLinkResource)
        return res

    def get_text(self, alias: str) -> AliasedTextResource:
        res = self[alias]
        assert isinstance(res, AliasedTextResource)
        return res

    def get_table(self, alias: str) -> AliasedTableResource:
        res = self[alias]
        assert isinstance(res, AliasedTableResource)
        return res
