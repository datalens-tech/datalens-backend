from __future__ import annotations

import json
import logging
from typing import Sequence

from dl_constants.api_constants import DLHeadersCommon


LOGGER = logging.getLogger(__name__)


HEADER_LOGGING_CONTEXT = "X-DL-LoggingContext"
HEADER_DEBUG_MODE_ENABLED = "X-DL-Debug-Mode"
INTERNAL_HEADER_PROFILING_STACK = "X-DL-Profiling-Stack"

# Headers that will be stored to request context info
DEFAULT_RCI_PLAIN_HEADERS = (
    DLHeadersCommon.FORWARDED_FOR.value,
    "User-Agent",
    "X-DL-Folder-ID",
    DLHeadersCommon.TENANT_ID.value,
    DLHeadersCommon.WORKBOOK_ID.value,
    "Referer",
    "X-Chart-Id",
    "X-Dash-Id",
    "uber-trace-id",
    "Host",
    DLHeadersCommon.ACCEPT_LANGUAGE.value,
    # TODO FIX: Move to RCI attributes instead of headers passing
    DLHeadersCommon.SUDO.value,
    DLHeadersCommon.ALLOW_SUPERUSER.value,
)
DEFAULT_RCI_SECRET_HEADERS = (
    "Authorization",
    "Cookie",
    # TODO: BI-4918 move to local injection and reuse bi_api_commons_ya_cloud.constants.DLHeadersYC
    DLHeadersCommon.IAM_TOKEN.value,  # TODO add injection of extra secret headers to app factories
)


def normalize_header_name(hdr: str) -> str:
    return hdr.lower().replace("_", "-")


# TODO FIX: Add headers normalization function and use it here and in RCI handling middleware
def append_extra_headers_and_normalize(default: Sequence[str], extra: Sequence[str]) -> tuple[str, ...]:
    """Chains default and extra with CI deduplication. Translating result to lower case."""
    default_norm = tuple(normalize_header_name(hdr) for hdr in default)
    extra_norm = tuple(normalize_header_name(hdr) for hdr in extra)
    default_set = set(default_norm)
    return default_norm + tuple(hdr for hdr in extra_norm if hdr not in default_set)


def get_x_dl_context(header_value: str) -> dict:
    try:
        x_dl_context = json.loads(header_value)
    except (json.decoder.JSONDecodeError, TypeError):
        x_dl_context = {}
        LOGGER.warning(f"Got a malformed {DLHeadersCommon.DL_CONTEXT} header: {header_value!r}")

    x_dl_context_filtered = {k: v for k, v in x_dl_context.items() if isinstance(v, str)}

    if x_dl_context_filtered != x_dl_context:
        LOGGER.warning(f"Got unexpected data structure in {DLHeadersCommon.DL_CONTEXT} header: {header_value!r}")

    return x_dl_context_filtered
