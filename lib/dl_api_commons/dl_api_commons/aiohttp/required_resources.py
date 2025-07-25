from __future__ import annotations

import typing

from dl_api_commons.aiohttp.aiohttp_wrappers import (
    DLRequestBase,
    RequiredResourceCommon,
)


def get_required_resources(request: DLRequestBase) -> typing.FrozenSet[RequiredResourceCommon]:
    return typing.cast(typing.FrozenSet[RequiredResourceCommon], request.required_resources)
