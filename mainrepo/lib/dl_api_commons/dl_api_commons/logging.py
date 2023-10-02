from __future__ import annotations

import abc
from http.cookies import SimpleCookie
import logging
import os
import re
from typing import (
    Any,
    ClassVar,
    Iterable,
    Optional,
    Pattern,
    Sequence,
)

import attr

from dl_api_commons.headers import normalize_header_name
from dl_app_tools.log import context


LOGGER = logging.getLogger(__name__)

NON_TRANSITIVE_LOGGING_CTX_KEYS = frozenset(
    {
        "request_id",
        "pid",
    }
)

SECRET_HEADERS: frozenset[str] = frozenset(
    h.lower()
    for h in (
        "Authorization",
        "X-Us-Master-Token",
        "Master-Token",
        "X-DL-API-Key",
    )
)

SECRET_HEADERS_PATTERNS: Sequence[Pattern] = (re.compile(r".*token.*", re.IGNORECASE),)

SECRET_COOKIES: frozenset[str] = frozenset(
    c.lower()
    for c in (
        "Session_id",
        "sessionid2",
        "yc_session",
        "iam_cookie",
    )
)


def _is_secret_header(header_name: str) -> bool:
    if header_name.lower() in SECRET_HEADERS:
        return True

    for pattern in SECRET_HEADERS_PATTERNS:
        if pattern.match(header_name):
            return True

    return False


def _obfuscate_value(secret_value: str) -> str:
    repl_str = "<hidden>"
    if len(secret_value) > 8:
        return secret_value[:3] + repl_str + secret_value[-3:]
    else:
        return repl_str


def _obfuscate_cookie_header_value(cookie_string: str) -> str:
    cookie: SimpleCookie = SimpleCookie(cookie_string)
    for cookie_name in cookie:
        if cookie_name.lower() in SECRET_COOKIES:
            repl_str = "<hidden>"
            cookie[cookie_name].set(cookie_name, repl_str, repl_str)

    return cookie.output(header="", sep=";").strip()


def clean_secret_data_in_headers(headers: Iterable[tuple[str, str]]) -> Iterable[tuple[str, str]]:
    return tuple(
        (
            name,
            _obfuscate_value(val)
            if _is_secret_header(name)
            else _obfuscate_cookie_header_value(val)
            if name.lower() == "cookie"
            else val,
        )
        for name, val in headers
    )


def log_request_start(logger: logging.Logger, method: str, full_path: str, headers: Iterable[tuple[str, str]]) -> None:
    clean_headers = clean_secret_data_in_headers(headers)

    logger.info(
        "Received request. method: %s, path: %s, headers: %s, pid: %s",
        method.upper(),
        full_path,
        # Complexity to be compatible with previous version of logger
        "{{{}}}".format(", ".join([f"{k!r}: {v!r}" for k, v in clean_headers])),
        os.getpid(),
    )


def log_request_end(logger: logging.Logger, method: str, full_path: str, status_code: int) -> None:
    logger.info(
        "Response. method: %s, path: %s, status: %d",
        method.upper(),
        full_path,
        status_code,
        extra=dict(
            event_code="http_response",
            request_method=method,
            request_path=full_path,
            response_status=status_code,
        ),
    )


# TODO FIX: Make more strict typing for headers
def _normalize_headers(headers: Any) -> Optional[dict[str, str]]:
    if headers is None:
        return None
    if hasattr(headers, "items"):
        headers = headers.items()
    headers = sorted(headers)
    headers = {normalize_header_name(key): value for key, value in headers}

    headers = dict(clean_secret_data_in_headers(headers.items()))

    return headers


# TODO CONSIDER: Create custom type for headers
def log_request_end_extended(
    logger: logging.Logger,
    request_method: str,
    request_path: str,
    request_headers: Optional[dict],
    response_status: int,
    response_headers: Optional[dict],
    response_timing: Optional[float],
    # ...
    user_id: Optional[str] = None,
    username: Optional[str] = None,
    # extra extra for when they're not in the context.
    # TODO: tenant_id
    request_id: Optional[str] = None,
    endpoint_code: Optional[str] = None,
) -> None:
    """
    Response pre-return detailed (extended) logging.

    :param logger: logger to send the log to.

    :param request_method: HTTP method; uppercase, e.g. 'GET'.

    :param request_path: HTTP path; should, generally, include the query string,
        i.e. '/a/b?c=d&e=f'.

    :param request_headers: HTTP headers, unordered, normalized (lowercase
        dash names, i.e. 'user-agent'), unduplicated, scrubbed.
        Headers like 'remote-addr' should be authoritative.

    :param user_id: primary identifier of the *authorized* initial user (authoritative);
        e.g. '1120000000092758'.

    :param username: readable identifier of the user, i.e. login, e.g. 'robot-datalens'.

    :param response_status: HTTP status of the response; e.g. '502'.

    :param response_headers: HTTP headers of the response, normalized.

    :param response_timing: duration of the request handling, in seconds.

    :param request_id: unique request identifier; should be specified when not
        available in context.

    :param endpoint_code: name of the handler; should be specified when not
        available in context; use an empty string if it is not applicable.
    """
    request_method = request_method.upper()
    request_headers = _normalize_headers(request_headers)
    response_headers = _normalize_headers(response_headers)
    if response_timing is not None:
        response_timing = round(response_timing, 4)

    extra = dict(
        event_code="http_response",
        request_method=request_method,
        request_path=request_path,
        request_headers=request_headers,
        user_id=user_id,
        username=username,
        response_status=response_status,
        response_headers=response_headers,
        response_timing=response_timing,
        # Other possibilities:
        # response_body_info=dict(body_piece=body[:max_size], body_size=body_size, ...),
        # response_details=dict(...),
    )
    if request_id is not None:
        extra["request_id"] = request_id
    if endpoint_code is not None:
        extra["endpoint_code"] = endpoint_code

    logger.info(
        "Response. method: %s, path: %s, status: %d", request_method, request_path, response_status, extra=extra
    )


class RequestLoggingContextController(metaclass=abc.ABCMeta):
    """
    Just an interface to mutate logging context (python logging, sentry, etc...)
    At this moment there is only one implementation: for sentry context.
    In near future it should completely replace log.context.log_context(...)
    """

    @abc.abstractmethod
    def put_to_context(self, key: str, value: Any) -> None:
        """
        For flexibility - any keys for this moment. Later key set will be restricted
        """


@attr.s
class CompositeLoggingContextController(RequestLoggingContextController):
    _sub_controllers: list[RequestLoggingContextController] = attr.ib(factory=list)

    def put_to_context(self, key: str, value: Any) -> None:
        for ctrl in self._sub_controllers:
            ctrl.put_to_context(key, value)

    def add_sub_controller(self, ctrl: RequestLoggingContextController) -> None:
        self._sub_controllers.append(ctrl)


def extra_with_evt_code(event_code: str, extra: dict[str, Any]) -> dict[str, Any]:
    return dict(extra, event_code=event_code)


def _get_map_key_label(*args: str, **kwargs: str) -> dict[str, str]:
    map_field_name_label: dict[str, str] = {}
    for field_name in args:
        map_field_name_label[field_name] = field_name
    for label, field_name in kwargs.items():
        map_field_name_label[field_name] = label

    return map_field_name_label


def format_dict(extra: dict[str, Any], separator: str = " ", *args: str, **kwargs: str) -> str:
    map_extra_name_label = _get_map_key_label(*args, **kwargs)
    parts = []

    for extra_name, label in map_extra_name_label.items():
        if extra_name in extra:
            parts.append(f"{label}={repr(extra[extra_name])}")
        else:
            parts.append(f"{label}=N/A")
            LOGGER.warning(
                "Can not found extra key during message formatting: %s",
                extra_name,
                extra=extra_with_evt_code("logging_missing_extra_in_formatting", dict(extra_name=extra_name)),
            )

    return separator.join(parts)


def mask_sensitive_fields_by_name_in_json_recursive(
    source: Optional[dict[str, Any]], extra_sensitive_key_names: Iterable[str] = ()
) -> Optional[dict[str, Any]]:
    if source is None:
        return None

    all_sensitive_key_names: set[str] = {
        "password",
        "token",
        "secret",
        "private_key",
        "cypher_text",
    }
    all_sensitive_key_names.update(extra_sensitive_key_names)

    def process_value(key_name: Optional[str], value: Any) -> Any:
        if value is None:
            return None

        if isinstance(value, dict):
            return {nested_key: process_value(nested_key, nested_value) for nested_key, nested_value in value.items()}
        if isinstance(
            value,
            (
                list,
                tuple,
            ),
        ):
            return [process_value(key_name, nested_value) for nested_value in value]
        if isinstance(value, str):
            if key_name in all_sensitive_key_names:
                return _obfuscate_value(value)
            return value

        if isinstance(
            value,
            (
                bool,
                float,
                int,
            ),
        ):
            if key_name in all_sensitive_key_names:
                LOGGER.error("Non-string type for sensitive field '%s': %s", key_name, type(value))
                return _obfuscate_value(str(value))
            return value

        raise TypeError(f"Unexpected value type: {type}")

    return process_value(None, source)


class LogRequestLoggingContextController(RequestLoggingContextController):
    allowed_keys: ClassVar[tuple[str, ...]] = (
        "request_id",
        "parent_request_id",
        "endpoint_code",
        "user_id",
        "folder_id",
        "project_id",
        "org_id",
    )

    def put_to_context(self, key: str, value: Any) -> None:
        if key in self.allowed_keys:
            # Each request assumed to be executed in individual ContextVars context so we don't need to pop it back
            # see `bi_core.flask_utils.context_var_middleware`
            context.put_to_context(key, value)
