from __future__ import annotations

from typing import Optional

import flask


def _ping_handler() -> str:
    return ""


"""
Handles the ping request if the request method is GET and the path is '/ping'.

This function checks if the incoming request is a GET request to the '/ping' endpoint.
If so, it delegates the handling to `_ping_handler()`. Otherwise, it returns None,
allowing Flask to fall through to other route handlers.

Returns:
    Optional[str]: The response from `_ping_handler()` if conditions are met, otherwise None.
"""


def _ping_handler_hax() -> Optional[str]:
    if flask.request.method == "GET" and flask.request.path.rstrip("/") == "/ping":
        return _ping_handler()
    return None  # flask: fall through
