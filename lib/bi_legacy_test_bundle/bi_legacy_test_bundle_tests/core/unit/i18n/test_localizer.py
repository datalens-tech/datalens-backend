# TODO: Move to dl_i18n

import os
from typing import Dict

import attr
import pytest

from dl_i18n.exc import (
    UnknownDomain,
    UnknownLocale,
)
from dl_i18n.localizer_base import (
    Translatable as BaseTranslatable,
    TranslationConfig,
    LocalizerLoader,
)


PATH = os.path.join(os.path.dirname(__file__), 'locales')


"""
ATTENZIONE!

Don't forget to update locale data
make update-po
make msgfmt
"""


@attr.s
class Translatable(BaseTranslatable):
    domain = attr.ib(default='i18n_test')


@pytest.fixture
def ua_localizer():
    loader = LocalizerLoader(
        configs=[
            TranslationConfig(
                path=PATH,
                domain='i18n_test',
                locale='ua',
            ),
        ],
    )
    factory = loader.load()
    localizer = factory.get_for_locale('ua')
    return localizer


def test_translate_dict(ua_localizer):
    data: Dict[str, Translatable] = {
        'foo': Translatable('example2'),
    }

    text = data['foo']
    assert ua_localizer.translate(text) == 'приклад2'


def test_translate_class(ua_localizer):
    @attr.s
    class Foo:
        foo: Translatable = attr.ib(default=Translatable('some string'))

    text = Foo().foo
    assert ua_localizer.translate(text) == 'якийсь рядок'


def test_multiple_locales():
    loader = LocalizerLoader(
        configs=[
            TranslationConfig(
                path=PATH,
                domain='i18n_test',
                locale='ua',
            ),
            TranslationConfig(
                path=PATH,
                domain='i18n_test',
                locale='en',
            ),
        ],
    )
    some_test_string = Translatable('Do you understand me?')
    factory = loader.load()
    assert factory.get_for_locale('ua').translate(some_test_string) == 'Ви мене розумієте?'
    assert factory.get_for_locale('en').translate(some_test_string) == 'Do you understand me, sir?'


def test_fallback():
    loader = LocalizerLoader(
        configs=[
            TranslationConfig(
                path=PATH,
                domain='i18n_test',
                locale='ua',
            ),
        ],
    )
    some_test_string = Translatable('test fallback')
    factory = loader.load()
    fallback_localizer = factory.get_for_locale('ua')
    localizer = factory.get_for_locale('fr', fallback=fallback_localizer)
    assert localizer.translate(some_test_string) == 'Кивні, якщо мене не зрозумієш'

    # and without fallback
    with pytest.raises(UnknownLocale):
        factory.get_for_locale('fr')


def test_translate_unknown_string(ua_localizer):
    # gettext extract translatable strings by regexp
    # so this hack should work
    StringWithoutGettext = Translatable
    text = StringWithoutGettext('unknown string')
    assert ua_localizer.translate(text) == 'unknown string'


def test_unknown_domain():
    loader = LocalizerLoader(
        configs=[
            TranslationConfig(
                path=PATH,
                domain='i18n_test1',
                locale='ua',
            ),
        ],
    )
    with pytest.raises(UnknownDomain):
        loader.load()
