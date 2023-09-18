import os

from dl_formula_ref.i18n.registry import DOMAIN
from dl_i18n.localizer_base import TranslationConfig

_LOCALE_DIR = os.path.join(os.path.dirname(__file__), "../..", "locales")

CONFIGS = [
    TranslationConfig(path=_LOCALE_DIR, domain=DOMAIN, locale="en"),
    TranslationConfig(path=_LOCALE_DIR, domain=DOMAIN, locale="ru"),
]
