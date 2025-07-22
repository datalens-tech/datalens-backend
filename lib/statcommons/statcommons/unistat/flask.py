"""
flask as http daemon for unistat handling.
"""

from __future__ import (
    absolute_import,
    division,
    print_function,
    unicode_literals,
)

from collections import OrderedDict
import os

import flask

from .common import (
    dump_for_prometheus,
    results_to_response,
)
from .uwsgi import (
    uwsgi_prometheus,
    uwsgi_unistat,
)


def unistat_handler():
    results = OrderedDict()
    if os.environ.get("UWSGI_STATS") and os.environ.get("QLOUD_APPLICATION"):
        results.update(uwsgi_unistat())
    data = "".join(results_to_response(results))
    return flask.Response(data, content_type="application/json; charset=utf-8")


def register_unistat(app):
    return app.route("/unistat")(unistat_handler)


def _unistat_handler_hax():
    if flask.request.method == "GET" and flask.request.path.rstrip("/") == "/unistat":
        return unistat_handler()
    return None  # flask: fall through


def metrics_handler():
    data = uwsgi_prometheus()
    body = "".join(dump_for_prometheus(data))
    return flask.make_response(body)
