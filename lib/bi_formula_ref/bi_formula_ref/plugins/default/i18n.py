import os

from bi_i18n.localizer_base import TranslationConfig

from bi_formula_ref.i18n.registry import DOMAIN


_LOCALE_DIR = os.path.join(os.path.dirname(__file__), '../..', 'locales')

CONFIGS = [
    TranslationConfig(path=_LOCALE_DIR, domain=DOMAIN, locale='en_US'),
    TranslationConfig(path=_LOCALE_DIR, domain=DOMAIN, locale='ru_RU'),
]
