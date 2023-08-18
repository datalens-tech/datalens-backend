#!/usr/bin/env python3
# coding: utf8
'''
...
'''

# from __future__ import annotations

from __future__ import annotations

from ..settings import settings
# pylint: disable=unused-import
from . import (
    base_app,
    views_internal,
    views_public,
    views_auxiliary,
)
from .base_app import LOGGER, app


__all__ = (
    'base_app', 'views_internal', 'views_public', 'views_auxiliary',
    'LOGGER', 'app',
)


def main_i(host='::', port=int(settings.HTTP_PORT), **kwargs):
    LOGGER.debug("app.run(host=%r, port=%r, **%r)", host, port, kwargs)
    return app.run(host=host, port=port, **kwargs)


main = main_i  # TODO?: cmdline params parsing


if __name__ == '__main__':
    main()
