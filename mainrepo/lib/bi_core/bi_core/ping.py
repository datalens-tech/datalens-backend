from __future__ import annotations

import flask


def _ping_handler():  # type: ignore  # TODO: fix
    return ''


def register_ping_handler(app):  # type: ignore  # TODO: fix
    return app.route('/ping')(_ping_handler)


def _ping_handler_hax():  # type: ignore  # TODO: fix
    if flask.request.method == 'GET' and flask.request.path.rstrip('/') == '/ping':
        return _ping_handler()
    return None  # flask: fall through


def register_ping_handler_hax(app):  # type: ignore  # TODO: fix
    # HAX to avoid bumping into inappropriate authorization added in 'before_request'.
    app.before_request(_ping_handler_hax)


# compat, to be removed alongside the whole 'hax'.
PingHandler = register_ping_handler_hax
