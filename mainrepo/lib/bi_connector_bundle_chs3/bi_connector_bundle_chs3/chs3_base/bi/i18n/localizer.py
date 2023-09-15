import os

import attr

from bi_i18n.localizer_base import Translatable as BaseTranslatable
from bi_i18n.localizer_base import TranslationConfig

import bi_connector_bundle_chs3 as package

DOMAIN = f'{package.__name__}'
CONFIGS = [
    TranslationConfig(
        path=os.path.relpath(os.path.join(os.path.dirname(__file__), "../../../locales")),
        domain=DOMAIN,
        locale="en",
    ),
    TranslationConfig(
        path=os.path.relpath(os.path.join(os.path.dirname(__file__), "../../../locales")),
        domain=DOMAIN,
        locale="ru",
    ),
]


@attr.s
class Translatable(BaseTranslatable):
    domain: str = attr.ib(default=DOMAIN)
