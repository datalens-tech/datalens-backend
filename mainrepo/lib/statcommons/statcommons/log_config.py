
import os
import sys
import socket
import logging
import logging.config

from .logs import LOGMUTATORS


# Warning: an on-import network request; if there's a danger of a broken DNS
# setup, use `socket.gethostname()`.
HOSTNAME = socket.getfqdn()


ENV_CONTEXT = {
    'app_name': None,  # To be filled in on convigure.
    # e.g. 'sas1-9aab47fec64f.qloud-c.yandex.net'
    'hostname': HOSTNAME,
    # e.g. 'dataset-api-1.dataset-api.int-testing.bi.datalens.stable.qloud-d.yandex.net'
    'app_instance': os.environ.get('QLOUD_DISCOVERY_INSTANCE'),
    'app_version': os.environ.get('QLOUD_DOCKER_IMAGE'),
    # global IP addresses, maybe?
}


def make_file_logger_syslog(name, params=None, **kwargs):
    addr = '/dev/log-ext'
    if not os.path.exists(addr):
        addr = '/dev/log'
        if not os.path.exists(addr):
            return {'class': 'logging.NullHandler'}

    res = {
        'class': 'statcommons.logs.TaggedSysLogHandler',
        'address': addr,
        'syslog_tag': 'file__' + name,
    }
    if params:
        res.update(params)
    res.update(kwargs)
    return res


DEV_LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': True,
    'handlers': {
        'stream': {
            'class': 'logging.StreamHandler',
            'stream': sys.stderr,
            'level': 'DEBUG',
        },
        'stream_info': {
            'class': 'logging.StreamHandler',
            'stream': sys.stderr,
            'level': 'INFO',
        },
    },
    'loggers': {
    },
    'root': {
        'level': 'INFO',
        'handlers': ['stream'],
    },
}


BASE_LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'filters': {
        'events': {'()': 'logging.Filter', 'name': 'events'},
        'fastlogs': {'()': 'logging.Filter', 'name': 'fastlogs'},
    },
    'formatters': {
        'verbose': {'format': '[%(asctime)s] %(levelname)s: %(name)s: %(message)s'},
        # Put any preferred stdout/stderr json formatter in there:
        'json': {'format': ''},
        'jsonext': {'()': 'statcommons.logs.JsonExtFormatter'},
        'jsonevent': {'()': 'statcommons.logs.JsonEventFormatter'},
    },
    'handlers': {
        'stream': {
            'class': 'logging.StreamHandler',
            'formatter': 'json',
            'stream': sys.stdout,
            'level': 'DEBUG',
        },
        'stream_info': {
            'class': 'logging.StreamHandler',
            'formatter': 'json',
            'stream': sys.stdout,
            'level': 'INFO',
        },
        'stream_err': {
            'class': 'logging.StreamHandler',
            'formatter': 'json',
            'stream': sys.stdout,
            'level': 'ERROR',
        },
        'debug_log': make_file_logger_syslog(
            'debug',
            formatter='jsonext',
            level='DEBUG',
        ),
        'fast_log': make_file_logger_syslog(
            'fast',
            formatter='jsonext',
            level='DEBUG',
            # Ways to use these:
            #  * Use logging.getLogger('fastlogs')
            #  * Replace the `filters` setting with your own.
            filters=['fastlogs'],
        ),
        'event_log': make_file_logger_syslog(
            'event',
            formatter='jsonevent',
            level='DEBUG',
            filters=['events'],
        ),
    },
    'loggers': {
    },
    'root': {
        'level': 'DEBUG',
        'handlers': ['debug_log', 'fast_log', 'event_log'],
    },
}


def add_env_context_logmutator(record):
    for key, val in ENV_CONTEXT.items():
        setattr(record, key, val)


def configure_logging(app_name, cfg, use_env_context=True):
    current_name = ENV_CONTEXT.get('app_name')
    if current_name is not None and current_name != app_name:
        raise Exception("Attempting to configure logging again with a different app name", current_name, app_name)

    ENV_CONTEXT['app_name'] = app_name

    logging.config.dictConfig(cfg)
    LOGMUTATORS.apply(require=False)
    if use_env_context:
        LOGMUTATORS.add_mutator('env_context', add_env_context_logmutator)


def deconfigure_logging():
    """
    Primarily this is a hack for tests, particularly for running RQE under UWSGI after fork().
    As it is, it doesn't deconfigure the logging completely
    """
    ENV_CONTEXT.pop('app_name', None)


if __name__ == '__main__':
    # small self-test
    configure_logging(app_name='statcommonstst', cfg=BASE_LOGGING_CONFIG)
    configure_logging(app_name='statcommonstst', cfg=DEV_LOGGING_CONFIG)
    logging.getLogger().info('set up.')
