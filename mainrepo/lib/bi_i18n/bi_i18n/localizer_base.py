from collections import defaultdict
import gettext
import logging
from typing import (
    Iterable,
    Optional,
)

import attr

from bi_i18n.exc import (
    UnknownDomain,
    UnknownLocale,
)

LOGGER = logging.getLogger(__name__)


@attr.s
class Translatable:
    _s: str = attr.ib()
    domain: str = attr.ib()

    def to_text(self) -> str:
        return self._s


@attr.s(frozen=True)
class TranslationConfig:
    path: str = attr.ib()
    locale: str = attr.ib()
    domain: str = attr.ib()

    @property
    def key(self) -> tuple:
        return self.domain, self.locale


@attr.s
class _GNULocalizer:
    domain: str = attr.ib()
    locale: str = attr.ib()
    _localizer: gettext.GNUTranslations = attr.ib()

    def translate(self, message: Translatable) -> str:
        return self._localizer.gettext(message.to_text())


class Localizer:
    def __init__(self, localizers: Iterable[_GNULocalizer]):
        self._localizers = {}
        for localizer in localizers:
            self._localizers[localizer.domain] = localizer

    def translate(self, message: Translatable) -> str:
        if message.domain not in self._localizers:
            raise UnknownDomain(f"Unknown domain {message.domain}")
        return self._localizers[message.domain].translate(message)


class LocalizerFactory:
    def __init__(self, localizers: Iterable[_GNULocalizer]):
        self._localizers: dict[str, list[_GNULocalizer]] = defaultdict(list)
        for localizer in localizers:
            self._localizers[localizer.locale].append(localizer)

    def get_for_locale(self, locale: str, fallback: Optional[Localizer] = None) -> Localizer:
        localizers = self._localizers.get(locale)
        if localizers is None:
            LOGGER.info("Unknown locale %s", locale)
            if fallback:
                return fallback
            raise UnknownLocale(f"Unknown locale {locale}")
        return Localizer(localizers)


@attr.s
class LocalizerLoader:
    _configs: Iterable[TranslationConfig] = attr.ib()

    def load(self) -> LocalizerFactory:
        localizers: list[_GNULocalizer] = []
        domain_locales: dict[tuple, int] = defaultdict(int)
        for config in self._configs:
            try:
                gnu_localizer = gettext.translation(
                    config.domain,
                    localedir=config.path,
                    languages=[config.locale],
                )
                assert isinstance(gnu_localizer, gettext.GNUTranslations)
            except FileNotFoundError:
                raise UnknownDomain(f"Unknown domain {config.domain}")
            localizers.append(
                _GNULocalizer(
                    domain=config.domain,
                    locale=config.locale,
                    localizer=gnu_localizer,
                ),
            )
            domain_locales[config.key] += 1
        duplicated_domain_locales = ["-".join(key) for key, value in domain_locales.items() if value > 1]
        if duplicated_domain_locales:
            LOGGER.warning("Duplicated domain-locale pairs: %s", ";".join(duplicated_domain_locales))
        return LocalizerFactory(
            localizers=localizers,
        )
