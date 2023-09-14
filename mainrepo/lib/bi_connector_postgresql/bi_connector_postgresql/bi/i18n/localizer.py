import os

import attr

from bi_i18n.localizer_base import Translatable as BaseTranslatable
from bi_i18n.localizer_base import TranslationConfig

DOMAIN = "bi_connector_postgresql"
CONFIGS = [
    TranslationConfig(
        path=os.path.relpath(os.path.join(os.path.dirname(__file__), "../../locales")),
        domain=DOMAIN,
        locale="en",
    ),
    TranslationConfig(
        path=os.path.relpath(os.path.join(os.path.dirname(__file__), "../../locales")),
        domain=DOMAIN,
        locale="ru",
    ),
]


@attr.s
class Translatable(BaseTranslatable):
    domain: str = attr.ib(default=DOMAIN)
