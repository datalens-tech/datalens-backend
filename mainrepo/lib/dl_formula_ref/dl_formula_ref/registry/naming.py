from __future__ import annotations

import abc
from typing import (
    TYPE_CHECKING,
    Optional,
)

import attr

from dl_formula_ref.i18n.registry import get_localizer
from dl_formula_ref.texts import FUNCTION_CATEGORY_TAG


if TYPE_CHECKING:
    import dl_formula_ref.registry.base as _registry_base


def translate(text: Optional[str], locale: str) -> str:
    # Note: for `text=None` returns an empty string `''`
    gtext = get_localizer(locale).translate
    return gtext(text) if text else ""


class NamingProviderBase(abc.ABC):
    @abc.abstractmethod
    def get_internal_name(self, item: _registry_base.FunctionDocRegistryItem) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_title(self, item: _registry_base.FunctionDocRegistryItem, locale: str) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_short_title(self, item: _registry_base.FunctionDocRegistryItem, locale: str) -> str:
        raise NotImplementedError()


class DefaultNamingProvider(NamingProviderBase):
    def get_internal_name(self, item: _registry_base.FunctionDocRegistryItem) -> str:
        return item.name.upper()

    def get_title(self, item: _registry_base.FunctionDocRegistryItem, locale: str) -> str:
        return item.name.upper()

    def get_short_title(self, item: _registry_base.FunctionDocRegistryItem, locale: str) -> str:
        return item.name.upper()


@attr.s
class CustomNamingProvider(DefaultNamingProvider):
    _internal_name: Optional[str] = attr.ib(kw_only=True, default=None)
    _title: Optional[str] = attr.ib(kw_only=True, default=None)
    _short_title: Optional[str] = attr.ib(kw_only=True, default=None)

    def get_internal_name(self, item: _registry_base.FunctionDocRegistryItem) -> str:
        if self._internal_name is not None:
            return self._internal_name
        return super().get_internal_name(item)

    def get_title(self, item: _registry_base.FunctionDocRegistryItem, locale: str) -> str:
        if self._title is not None:
            return translate(self._title, locale=locale)
        return super().get_title(item, locale=locale)

    def get_short_title(self, item: _registry_base.FunctionDocRegistryItem, locale: str) -> str:
        if self._short_title is not None:
            return translate(self._short_title, locale=locale)
        if self._title is not None:
            return translate(self._title, locale=locale)
        return super().get_short_title(item, locale=locale)


class CategoryPostfixNamingProvider(CustomNamingProvider):
    def get_title(self, item: _registry_base.FunctionDocRegistryItem, locale: str) -> str:
        if self._title is not None:
            return translate(self._title, locale=locale)
        return f"{item.name.upper()} ({translate(FUNCTION_CATEGORY_TAG[item.category_name], locale=locale)})"
