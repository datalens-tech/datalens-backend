from __future__ import annotations

import logging
import re
from typing import Any

from dl_api_commons.logging import RequestObfuscator
from dl_obfuscator import (
    ObfuscationContext,
    get_request_obfuscation_engine,
)


log = logging.getLogger()

SECRET_VAR_RE = re.compile(
    "(?i)^.*("
    "token"
    "|"
    "fernet"
    "|"
    "password"
    "|"
    "pwd"
    "|"
    "secret"
    "|"
    "cypher"
    "|"
    "master_key"
    "|"
    "api_key"
    ").*$"
)

SECRET_VAR_CONTENT_RE = re.compile(
    "(?i).*("
    "BEGIN PRIVATE KEY"
    "|"
    "yc_session"
    "|"
    "CheckSessionRequest"  # GRPC request class that contain in repr secret cookies
    ")"
)

S3_TBL_FUNC_RE = re.compile("s3\([^,]*['\"]http([^,]+,){3}")  # noqa


def is_secret_var(var_name: str) -> bool:
    return bool(SECRET_VAR_RE.match(var_name))


def get_content_repr(content: Any) -> str:
    if isinstance(content, str):
        return content
    return repr(content)


def is_secret_content(content: Any) -> bool:
    if content is None:
        return False
    content_repr = get_content_repr(content)
    return bool(SECRET_VAR_CONTENT_RE.match(content_repr))


def contains_s3_tbl_func(content: Any) -> bool:
    if content is None:
        return False
    content_repr = get_content_repr(content)
    return bool(S3_TBL_FUNC_RE.search(content_repr))


def mask_s3_tbl_func(content: Any) -> str:
    content_repr = get_content_repr(content)
    while match := S3_TBL_FUNC_RE.search(content_repr):
        content_repr = content_repr[: match.start() + 3] + "<hidden>" + content_repr[match.end() :]
    return content_repr


def cleanup_local_vars(local_vars: dict) -> None:
    """Mutates local vars dict inplace"""
    for name, val in local_vars.items():
        if is_secret_var(name) or is_secret_content(val):
            local_vars[name] = "[hidden]"
        elif contains_s3_tbl_func(val):
            local_vars[name] = mask_s3_tbl_func(val)


def cleanup_event_headers(original_headers: dict[str, str]) -> dict[str, str]:
    return {
        name: value
        for name, value in RequestObfuscator().clean_secret_data_in_headers(
            (
                original_name,
                original_value,
            )
            for original_name, original_value in original_headers.items()
        )
    }


def cleanup_event_request_section(req_section: dict[str, str | dict[str, str]]) -> dict[str, str | dict[str, str]]:
    clean_req_section = dict(req_section)

    secret_original_headers = req_section.get("headers")

    if secret_original_headers is None:
        pass
    elif isinstance(secret_original_headers, dict):
        clean_req_section["headers"] = cleanup_event_headers(secret_original_headers)
    else:
        log.error(f"Unexpected type of request headers section in outgoing Sentry event: {type(req_section)}")

    return clean_req_section


def cleanup_common_secret_data(
    event: dict,
    hint: dict,  # noqa
) -> dict:
    for exc_data in event.get("exception", {}).get("values"):
        for frame in exc_data.get("stacktrace", {}).get("frames", ()):
            local_vars = frame.get("vars", {})
            cleanup_local_vars(local_vars)

    secret_original_req_section = event.get("request")

    if secret_original_req_section is None:
        pass
    elif isinstance(secret_original_req_section, dict):
        event["request"] = cleanup_event_request_section(secret_original_req_section)
    else:
        log.error(f"Unexpected type of request section in outgoing Sentry event: {type(secret_original_req_section)}")

    _obfuscate_sentry_event(event)

    return event


def _obfuscate_sentry_event(event: dict) -> None:
    """Apply obfuscation engine to Sentry event message and exception values."""
    engine = get_request_obfuscation_engine()
    if engine is None:
        return

    if "message" in event and isinstance(event["message"], str):
        event["message"] = engine.obfuscate(event["message"], ObfuscationContext.SENTRY)

    if "exception" in event and "values" in event["exception"]:
        for exc in event["exception"]["values"]:
            if "value" in exc and isinstance(exc["value"], str):
                exc["value"] = engine.obfuscate(exc["value"], ObfuscationContext.SENTRY)

    for breadcrumb in event.get("breadcrumbs", {}).get("values", []):
        if "message" in breadcrumb and isinstance(breadcrumb["message"], str):
            breadcrumb["message"] = engine.obfuscate(breadcrumb["message"], ObfuscationContext.SENTRY)
        if "data" in breadcrumb and isinstance(breadcrumb["data"], dict):
            breadcrumb["data"] = engine.obfuscate(breadcrumb["data"], ObfuscationContext.SENTRY)

    req = event.get("request")
    if req is not None and isinstance(req, dict):
        if "url" in req and isinstance(req["url"], str):
            req["url"] = engine.obfuscate(req["url"], ObfuscationContext.SENTRY)
        if "query_string" in req and isinstance(req["query_string"], str):
            req["query_string"] = engine.obfuscate(req["query_string"], ObfuscationContext.SENTRY)