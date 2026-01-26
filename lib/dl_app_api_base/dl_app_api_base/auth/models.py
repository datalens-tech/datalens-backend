import logging
import re

import attr


LOGGER = logging.getLogger(__name__)

_DEFAULT_ROUTE_MATCHER_METHODS = frozenset(["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"])


@attr.define(frozen=True, kw_only=True)
class RouteMatcher:
    path_regex: re.Pattern[str]
    methods: frozenset[str] = attr.ib(default=_DEFAULT_ROUTE_MATCHER_METHODS)

    def matches(
        self,
        route: str,
        method: str,
    ) -> bool:
        return self.path_regex.match(route) is not None and method in self.methods
