# TODO: Remove or refactor

import gettext


DEFAULT_LOCALE = 'en'


def get_locales() -> list[str]:
    return ['en', 'ru']  # TODO: pluginize


def get_gettext():
    if '_' not in globals():
        gettext.install("app")
    return _  # type: ignore  # TODO: fix  # noqa
