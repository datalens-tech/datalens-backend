""" ... """

from __future__ import annotations

from functools import wraps
import json
import logging
import sys
from typing import (
    Callable,
    Optional,
)

import flask
from flask import current_app as capp
from flask import request
from flask_restx import Namespace
from marshmallow import Schema
from marshmallow import ValidationError as MValidationError
from werkzeug.exceptions import HTTPException

from dl_api_commons.logging import RequestObfuscator
from dl_api_lib import utils
from dl_api_lib.error_handling import (
    BIError,
    RegularAPIErrorSchema,
    status,
)
from dl_api_lib.schemas.main import get_api_model


LOGGER = logging.getLogger(__name__)


def abort_request(code, message=None, response_data: Optional[dict] = None) -> None:  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
    """
    Copy-paste of `flask_restx.abort`, but without passing additional data through **kwargs.
    So we can to pass field "code" to response json.

    :param int code: The associated HTTP status code
    :param str message: An optional details message
    :param response_data: Any additional data to pass to the error payload
    :raise HTTPException:
    """
    if response_data is None:
        response_data = {}
    try:
        flask.abort(code)
    except HTTPException as e:
        if message:
            response_data["message"] = str(message)
        if response_data:
            e.data = response_data  # type: ignore  # TODO: fix
        raise


def schematic_request(  # type: ignore  # TODO: fix
    ns: Namespace,
    body: Optional[Schema] = None,
    query: Optional[Schema] = None,
    responses: Optional[dict[int, tuple[str, Schema]]] = None,
    dump: bool = True,
):
    """
    Decorator for REST API handlers.
    Allows the specification of marshmallow schemas for:
    - request body
    - request query
    - responses

    Translates exception classes into HTTP status codes
    """
    responses = responses or {}
    api_model_responses = {
        code: (desc, get_api_model(model, ns) if model is not None else None)
        for code, (desc, model) in responses.items()
    }
    body_schema = body
    query_schema = query

    def decorator(f):  # type: ignore  # TODO: fix
        if responses:
            for code, (description, model) in api_model_responses.items():
                f = ns.response(code, description, model)(f)

        @wraps(f)
        def wrapper(*args, **kwargs):  # type: ignore  # TODO: fix
            body = request.get_json() if body_schema is not None else None

            if LOGGER.isEnabledFor(logging.INFO):
                dbg_body_data = RequestObfuscator().mask_sensitive_fields_by_name_in_json_recursive(body)
                dbg_body = json.dumps(dbg_body_data)
                url = request.url
                pfx = "http://"
                if url.startswith(pfx):
                    url = "https://" + url[len(pfx) :]
                LOGGER.info("Body (piece) for %s: %s...", url, dbg_body[:1000])
                extra = dict(
                    request_path=request.full_path,
                    # Tricky point:
                    # Can either use the `dbg_body` as a string, pre-dumped
                    # into json, in which case it will be handled as a
                    # continuous homogenous string.
                    # or give the `dbg_body_data` to the logger, which will
                    # dump it into json, then, in the logfeller, it will be
                    # loaded (full structure) into memory and then dumped into
                    # YSON.
                    request_body=dbg_body,
                )
                LOGGER.debug("Body for %s: %s...", url, dbg_body[:100], extra=extra)
                del dbg_body_data
                del dbg_body

            if query_schema is not None:
                try:
                    kwargs["query"] = query_schema.load(request.args)
                except MValidationError as err:
                    abort_request(status.BAD_REQUEST, err.messages)

            if body_schema is not None:
                try:
                    kwargs["body"] = body_schema.load(body)
                except MValidationError as err:
                    LOGGER.exception("Validation errors")
                    abort_request(status.BAD_REQUEST, err.messages)

            try:
                result = f(*args, **kwargs)
                if isinstance(result, tuple) and len(result) == 2:
                    result, code = result
                else:
                    code = status.OK
                code = int(code)
                if dump and code in responses:  # type: ignore  # TODO: fix
                    response_schema = responses[code][1]  # type: ignore  # TODO: fix
                    if response_schema:
                        result = response_schema.dump(result)
            except HTTPException:
                # which is the exception raised by flask's `abort`
                raise
            except Exception as err:
                _ei = sys.exc_info()

                # TODO FIX: Make fallback for exceptions during creating error response
                bi_error = BIError.from_exception(err)
                error_info = RegularAPIErrorSchema().dump(bi_error)
                error_code = bi_error.http_code

                if error_code is None:
                    error_code = status.INTERNAL_SERVER_ERROR
                    LOGGER.exception("Caught an exception in request handler")
                else:
                    LOGGER.info("Regular exception fired", exc_info=True)

                abort_request(error_code, response_data=error_info)

            return result, code

        if body_schema is not None:
            # decorate wrapper
            wrapper = ns.doc(body=get_api_model(body_schema, ns), responses=api_model_responses)(wrapper)

        return wrapper

    return decorator


def with_profiler_stats(stats_dir: str, condition_check: Callable = None):  # type: ignore  # TODO: fix
    """Run function with cProfiler and save stats to file"""

    def decorator(func):  # type: ignore  # TODO: fix
        @wraps(func)
        def wrapper(*args, **kwargs):  # type: ignore  # TODO: fix
            if condition_check is None or condition_check(*args, **kwargs):
                with utils.profile_stats(stats_dir):
                    return func(*args, **kwargs)
            else:
                return func(*args, **kwargs)

        return wrapper

    return decorator
