"""
Prepare profiling stack for flask application
"""

from __future__ import annotations

from flask import (
    Flask,
    request,
)

from bi_api_commons import headers
from bi_app_tools.log import context
from bi_app_tools.profiling_base import GenericProfiler


def set_up(app: Flask, accept_outer_stages=False, **kwargs):  # type: ignore  # TODO: fix
    def pre_log_profiling_stack():  # type: ignore  # TODO: fix
        if accept_outer_stages:
            profiling_stack = request.headers.get(headers.INTERNAL_HEADER_PROFILING_STACK)
            GenericProfiler.reset_all(profiling_stack if profiling_stack else None)
        else:
            GenericProfiler.reset_all()

    def post_cleanup_context(exception=None):  # type: ignore  # TODO: fix
        context.pop_from_context("folder_id")

    app.before_request(pre_log_profiling_stack)
    app.teardown_request(post_cleanup_context)
