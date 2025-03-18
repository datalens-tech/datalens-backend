from __future__ import annotations

import enum
import typing

from flask import (
    current_app,
    request,
)


class RequiredResourceCommon(enum.Enum):
    US_HEADERS_TOKEN = enum.auto()
    SKIP_AUTH = enum.auto()


class RequiredResourceError(Exception):
    pass


def get_required_resources() -> typing.FrozenSet[RequiredResourceCommon]:
    if request.url_rule:
        view_func = current_app.view_functions.get(request.url_rule.endpoint, None)

    if hasattr(view_func, "view_class"):
        resource_class = view_func.view_class  # type: ignore  # 2025-03-18 # TODO: Item "None" of "Callable[..., Any] | None" has no attribute "view_class"  [union-attr]

    if hasattr(resource_class, "REQUIRED_RESOURCES"):
        return resource_class.REQUIRED_RESOURCES

    raise RequiredResourceError()
