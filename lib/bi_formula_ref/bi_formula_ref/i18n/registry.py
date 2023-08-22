from typing import Iterable

import attr

from bi_i18n.localizer_base import TranslationConfig, LocalizerLoader, Localizer, Translatable


_LOCALIZATION_CONFIGS: set[TranslationConfig] = set()

DOMAIN = 'bi_formula_ref'


def register_translation_configs(config: Iterable[TranslationConfig]) -> None:
    _LOCALIZATION_CONFIGS.update(config)


@attr.s
class _StringLocalizer:
    """
    An adapter for `Localizer`, which accepts only `Translatable` objects.
    """

    _translatable_localizer: Localizer = attr.ib(kw_only=True)
    _domain: str = attr.ib(kw_only=True)

    def translate(self, text: str) -> str:
        message = Translatable(text, domain=self._domain)
        return self._translatable_localizer.translate(message)


def get_localizer(locale: str):
    loc_loader = LocalizerLoader(configs=_LOCALIZATION_CONFIGS)
    loc_factory = loc_loader.load()
    localizer = _StringLocalizer(
        translatable_localizer=loc_factory.get_for_locale(locale=locale),
        domain=DOMAIN,
    )
    return localizer
