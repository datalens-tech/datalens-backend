"""
flask as http daemon for unistat handling.
"""

from __future__ import division, absolute_import, print_function, unicode_literals

import os
from collections import OrderedDict

import flask

from .common import results_to_response, dump_for_prometheus
from .uwsgi import uwsgi_unistat, uwsgi_prometheus


def unistat_handler():
    results = OrderedDict()
    if os.environ.get('UWSGI_STATS') and os.environ.get('QLOUD_APPLICATION'):
        results.update(uwsgi_unistat())
    data = ''.join(results_to_response(results))
    return flask.Response(data, content_type='application/json; charset=utf-8')


def register_unistat(app):
    return app.route('/unistat')(unistat_handler)


def _unistat_handler_hax():
    if flask.request.method == 'GET' and flask.request.path.rstrip('/') == '/unistat':
        return unistat_handler()
    return None  # flask: fall through


def register_unistat_hax(app):
    # HAX to avoid bumping into inappropriate authorization added in 'before_request'.
    app.before_request(_unistat_handler_hax)


def metrics_handler():
    data = uwsgi_prometheus()
    body = ''.join(dump_for_prometheus(data))
    return flask.make_response(body)


def register_metrics(app):
    return app.route('/metrics')(metrics_handler)
