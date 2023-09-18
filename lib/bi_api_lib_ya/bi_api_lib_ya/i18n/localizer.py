import os

import attr

import bi_api_lib_ya as package
from dl_i18n.localizer_base import Translatable as BaseTranslatable
from dl_i18n.localizer_base import TranslationConfig

DOMAIN = f"{package.__name__}"
CONFIGS = [
    TranslationConfig(
        path=os.path.relpath(os.path.join(os.path.dirname(__file__), "../locales")),
        domain=DOMAIN,
        locale="en",
    ),
    TranslationConfig(
        path=os.path.relpath(os.path.join(os.path.dirname(__file__), "../locales")),
        domain=DOMAIN,
        locale="ru",
    ),
]


@attr.s
class Translatable(BaseTranslatable):
    domain: str = attr.ib(default=DOMAIN)


_ = (
    Translatable("section_title-db"),
    Translatable("section_title-partner"),
    Translatable("section_title-files_and_services"),
)
