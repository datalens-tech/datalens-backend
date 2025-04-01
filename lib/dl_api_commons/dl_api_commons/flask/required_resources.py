from __future__ import annotations

import enum
import logging

from flask import (
    current_app,
    request,
)


logger = logging.getLogger(__name__)


class RequiredResourceCommon(enum.Enum):
    US_HEADERS_TOKEN = enum.auto()
    SKIP_AUTH = enum.auto()


def get_required_resources() -> frozenset[RequiredResourceCommon]:
    if request.url_rule:
        view_func = current_app.view_functions.get(request.url_rule.endpoint, None)
    else:
        logger.warning("Failed to get REQUIRED_RESOURCES: url_rule is not exist")
        return frozenset()

    if view_func and hasattr(view_func, "view_class"):
        resource_class = view_func.view_class
    else:
        logger.warning("Failed to get REQUIRED_RESOURCES: resource_class is not exist")
        return frozenset()

    if resource_class and hasattr(resource_class, "REQUIRED_RESOURCES"):
        return resource_class.REQUIRED_RESOURCES

    logger.warning("Failed to get REQUIRED_RESOURCES from flask request Resource")

    return frozenset()
