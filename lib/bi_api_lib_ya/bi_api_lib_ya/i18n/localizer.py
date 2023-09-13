import os

import attr

from bi_i18n.localizer_base import Translatable as BaseTranslatable, TranslationConfig


DOMAIN = 'bi_api_lib_ya'
CONFIGS = [
    TranslationConfig(
        path=os.path.relpath(os.path.join(os.path.dirname(__file__), '../locales')),
        domain=DOMAIN,
        locale='en',
    ),
    TranslationConfig(
        path=os.path.relpath(os.path.join(os.path.dirname(__file__), '../locales')),
        domain=DOMAIN,
        locale='ru',
    ),
]


@attr.s
class Translatable(BaseTranslatable):
    domain: str = attr.ib(default=DOMAIN)


_ = (
    Translatable('section_title-db'),
    Translatable('section_title-partner'),
    Translatable('section_title-files_and_services'),
)
