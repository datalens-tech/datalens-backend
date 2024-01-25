# TODO: Remove or refactor

import gettext


DEFAULT_LOCALE = "en"


def get_gettext():  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
    if "_" not in globals():
        gettext.install("app")
    return _  # type: ignore  # TODO: fix  # noqa
