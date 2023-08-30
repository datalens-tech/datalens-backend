import os

import attr

from bi_i18n.localizer_base import TranslationConfig, Translatable as BaseTranslatable


DOMAIN = 'bi_formula_ref_bi_connector_postgresql'

_LOCALE_DIR = os.path.join(os.path.dirname(__file__), '..', 'locales')

CONFIGS = [
    TranslationConfig(path=_LOCALE_DIR, domain=DOMAIN, locale='en'),
    TranslationConfig(path=_LOCALE_DIR, domain=DOMAIN, locale='ru'),
]

@attr.s
class Translatable(BaseTranslatable):
    domain: str = attr.ib(default=DOMAIN)
