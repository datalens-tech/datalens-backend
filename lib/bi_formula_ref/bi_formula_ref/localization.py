# TODO: Remove or refactor

import gettext


DEFAULT_LOCALE = 'en_US'


def get_locales() -> list[str]:
    return ['en_US', 'ru_RU']  # TODO: pluginize


def get_gettext():
    if '_' not in globals():
        gettext.install("app")
    return _  # type: ignore  # TODO: fix  # noqa
