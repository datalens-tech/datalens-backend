from __future__ import annotations

import abc
from collections import ChainMap
from typing import (
    Any,
    ItemsView,
    Iterable,
    MutableMapping,
    TypeVar,
)

import attr

from dl_i18n.localizer_base import Translatable


_RES_TV = TypeVar("_RES_TV", bound="AliasedResource")


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
class AliasedNullLinkResource(AliasedResource):
    """For overriding links"""

    url = None


@attr.s(frozen=True)
class AliasedTableResource(AliasedResource):
    table_body: list[list[str | Translatable]] = attr.ib(kw_only=True)


@attr.s(frozen=True)
class AliasedResourceRegistryBase(abc.ABC):
    @abc.abstractmethod
    def get_resources(self) -> MutableMapping[str, AliasedResource]:
        raise NotImplementedError

    def __getitem__(self, item: str) -> AliasedResource:
        return self.get_resources()[item]

    def __add__(self, other: AliasedResourceRegistryBase) -> AliasedResourceRegistryBase:
        return CompoundAliasedResourceRegistry(nested_registries=(self, other))

    def __contains__(self, item: str) -> bool:
        return item in self.get_resources()

    def items(self) -> ItemsView[str, AliasedResource]:
        return self.get_resources().items()

    def keys(self) -> Iterable[str]:
        return self.get_resources().keys()

    def get_link(self, alias: str) -> AliasedLinkResource | AliasedNullLinkResource:
        res = self[alias]
        assert isinstance(res, (AliasedLinkResource, AliasedNullLinkResource))
        return res

    def get_text(self, alias: str) -> AliasedTextResource:
        res = self[alias]
        assert isinstance(res, AliasedTextResource)
        return res

    def get_table(self, alias: str) -> AliasedTableResource:
        res = self[alias]
        assert isinstance(res, AliasedTableResource)
        return res


@attr.s(frozen=True)
class CompoundAliasedResourceRegistry(AliasedResourceRegistryBase):
    _nested_registries: tuple[AliasedResourceRegistryBase, ...] = attr.ib(default=())

    def get_resources(self) -> MutableMapping[str, AliasedResource]:
        return ChainMap(*(res_reg.get_resources() for res_reg in self._nested_registries))


@attr.s(frozen=True)
class SimpleAliasedResourceRegistry(AliasedResourceRegistryBase):
    _resources: MutableMapping[str, AliasedResource] = attr.ib(factory=dict)

    def get_resources(self) -> MutableMapping[str, AliasedResource]:
        return self._resources
