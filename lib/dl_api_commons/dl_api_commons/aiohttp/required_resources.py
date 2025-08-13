from __future__ import annotations

import typing

from dl_api_commons.aiohttp.aiohttp_wrappers import (
    DLRequestBase,
    RequiredResourceCommon,
)


def get_required_resources(request: DLRequestBase) -> frozenset[RequiredResourceCommon]:
    return typing.cast(frozenset[RequiredResourceCommon], request.required_resources)
