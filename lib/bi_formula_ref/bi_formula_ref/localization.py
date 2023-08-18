from __future__ import annotations

import gettext
import os
from functools import lru_cache
from typing import List


DEFAULT_LOCALE = 'en_US'

# This was supposed to be
# _LOCALE_DIR = pkg_resources.resource_filename('bi_formula', 'locales')
# but in tier0 the `resource_filename` adds a 'resfs/file/...' prefix, which doesn't work.
_LOCALE_DIR = os.path.join(os.path.dirname(__file__), 'locales')
DOMAIN = 'bi_formula_ref'


def get_locales() -> List[str]:
    # # Old code
    # return [
    #     name for name in os.listdir(_LOCALE_DIR)
    #     if os.path.isdir(os.path.join(_LOCALE_DIR, name))]
    # # Could actully get this from the resources list, but really,
    # # the simple way should be enough:
    return ['en_US', 'ru_RU']


def get_gettext():
    if '_' not in globals():
        gettext.install("app")
    return _  # type: ignore  # TODO: fix  # noqa


@lru_cache(maxsize=10)
def gettext_for_locale(locale: str) -> gettext.NullTranslations:
    return gettext.translation(
        domain=DOMAIN,
        localedir=_LOCALE_DIR,
        languages=[locale],
    )
