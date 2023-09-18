from typing import Iterable

import attr

import dl_formula_ref as package
from dl_i18n.localizer_base import (
    Localizer,
    LocalizerLoader,
    Translatable,
    TranslationConfig,
)

_LOCALIZATION_CONFIGS: set[TranslationConfig] = set()

DOMAIN = f'{package.__name__}_{package.__name__}'


def register_translation_configs(config: Iterable[TranslationConfig]) -> None:
    _LOCALIZATION_CONFIGS.update(config)


@attr.s
class _StringLocalizer:
    """
    An adapter for `Localizer`, which accepts only `Translatable` objects.
    """

    _translatable_localizer: Localizer = attr.ib(kw_only=True)
    _default_domain: str = attr.ib(kw_only=True)

    def translate(self, text: str | Translatable) -> str:
        if isinstance(text, str):
            text = Translatable(text, domain=self._default_domain)
        assert isinstance(text, Translatable)
        return self._translatable_localizer.translate(text)


def get_localizer(locale: str):
    loc_loader = LocalizerLoader(configs=_LOCALIZATION_CONFIGS)
    loc_factory = loc_loader.load()
    localizer = _StringLocalizer(
        translatable_localizer=loc_factory.get_for_locale(locale=locale),
        default_domain=DOMAIN,
    )
    return localizer
