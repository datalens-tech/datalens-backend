# coding: utf8
"""
...
"""


from __future__ import annotations

import sys
import logging

import statcommons.logs
import statcommons.log_config


LOGMUTATORS = statcommons.logs.LOGMUTATORS
HOSTNAME = statcommons.log_config.HOSTNAME
ENV_CONTEXT = statcommons.log_config.ENV_CONTEXT


def _init_dev_logging(level, **kwargs):
    try:
        from pyaux.runlib import init_logging as init_logging_pyaux
        init_logging_pyaux(level=level)
    except Exception:  # pylint: disable=broad-except
        logging.basicConfig(level=level)


class FastLogFilter(logging.Filter):

    def filter(self, record):
        name = record.name
        if name.startswith('fastlogs'):
            return True
        if name.startswith('yadls.httpjrpc.qutils.req'):
            return True
        return False


def _init_json_logging(level, extra_mutators=None, extra_debugs=(), app_name='dls'):
    from .settings import settings

    base = statcommons.log_config.BASE_LOGGING_CONFIG
    common_handlers = list(base['root']['handlers'])  # type: ignore  # TODO: fix
    # ^ ['debug_log', 'fast_log', 'event_log']

    common_handlers_config = {}

    sentry = bool(settings.SENTRY_DSN)
    if sentry:
        common_handlers += ['sentry']
        common_handlers_config['sentry'] = {
            'class': 'raven.handlers.logging.SentryHandler',
            'level': 'ERROR',
            'formatter': 'verbose',
            'dsn': settings.SENTRY_DSN,
        }

    errfile = bool(settings.ERRFILE_LOG)
    if errfile:
        common_handlers += ['errfile']
        common_handlers_config['errfile'] = {
            'class': 'logging.handlers.WatchedFileHandler',
            'formatter': 'json',
            'level': 'ERROR',
            'filename': '/var/log/app_err.log',
        }

    stdout_base = {
        'class': 'logging.StreamHandler',
        'formatter': 'json',
        'stream': sys.stdout,
    }

    def make_overridable_handler(key, high_level='info'):
        levelname = 'debug' if key in extra_debugs else high_level
        stdout_handler = 'stdout_{}'.format(levelname)
        handlers = [stdout_handler] + common_handlers
        return {'handlers': handlers, 'propagate': False}

    LOGGING = {
        **base,
        'disable_existing_loggers': False,
        'filters': {
            **base.get('filters', {}),  # type: ignore  # TODO: fix
            'fastlogs': {'()': 'yadls.runlib.FastLogFilter'},
        },
        'formatters': {
            **base.get('formatters', {}),  # type: ignore  # TODO: fix
            'json': {
                '()': 'yadls.logging.formatters.QloudJSONFormatter',
                'defaults': {'codebase': 'bi_dls_httpjrpc'},
            },
        },
        'handlers': {
            **base.get('handlers', {}),  # type: ignore  # TODO: fix
            **common_handlers_config,  # type: ignore  # TODO: fix
            # qloud
            'stdout_debug': dict(stdout_base, level=logging.DEBUG),
            'stdout_info': dict(stdout_base, level=logging.INFO),
            'stdout_warning': dict(stdout_base, level=logging.WARNING),
        },
        'loggers': {
            **base.get('loggers', {}),  # type: ignore  # TODO: fix
            'openapi_spec_validator': make_overridable_handler('openapi'),
            'hpack': make_overridable_handler('hpack'),
            'parso': make_overridable_handler('parso'),
            'aiopg': make_overridable_handler('aiopg', high_level='warning'),
        },
        'root': {
            **base.get('root', {}),  # type: ignore  # TODO: fix
            'handlers': (
                ['stdout_info' if level == logging.INFO else 'stdout_debug'] +
                common_handlers),
            'level': 'DEBUG',
        }
    }

    statcommons.log_config.configure_logging(app_name=app_name, cfg=LOGGING)

    if extra_mutators:
        for key, value in extra_mutators.items():
            LOGMUTATORS.add_mutator(key, value)


def init_logging(debug=True, json_stdout_log=None, **kwargs):
    if json_stdout_log is None:
        from .settings import settings
        json_stdout_log = settings.JSON_LOG

    loglevel = logging.DEBUG if debug else logging.INFO

    if json_stdout_log:
        _init_json_logging(level=loglevel, **kwargs)
    else:
        # no extra_logmutators / extra_debugs support.
        _init_dev_logging(level=loglevel, **kwargs)
