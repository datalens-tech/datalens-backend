"""
Query formatting for DBAPI-compliant interfaces
"""

from enum import Enum
from typing import ClassVar

import attr

from dl_constants.enums import UserDataType
from dl_dashsql.formatting.base import (
    JinjaStyleParamMatcher,
    QueryFormatter,
    QueryFormatterFactory,
    UnconsumedParameterPolicy,
    UnknownParameterPolicy,
)
from dl_dashsql.formatting.placeholder import (
    PlaceholderQueryFormatter,
    PlaceholderStyle,
)


class ParamStyle(Enum):
    """Python Database API paramstyle"""

    qmark = "qmark"
    numeric = "numeric"
    named = "named"
    format = "format"
    pyformat = "pyformat"


@attr.s(frozen=True)
class DBAPINamedPlaceholderStyle(PlaceholderStyle):
    """Placeholder style corresponding to the "named" DBAPI paramstyle"""

    def make_placeholder(self, param_name: str, param_idx: int, user_type: UserDataType) -> str:
        return f":{param_name}"


@attr.s(frozen=True)
class DBAPIQueryFormatterFactory(QueryFormatterFactory):
    _paramstyle: ParamStyle = attr.ib(kw_only=True, default=ParamStyle.named)

    _placeholder_style_map: ClassVar[dict[ParamStyle, PlaceholderStyle]] = {
        ParamStyle.named: DBAPINamedPlaceholderStyle(),
    }

    def get_query_formatter(self) -> QueryFormatter:
        query_formatter = PlaceholderQueryFormatter(
            unknown_param_policy=UnknownParameterPolicy.ignore,
            unconsumed_param_policy=UnconsumedParameterPolicy.ignore,
            param_matcher=JinjaStyleParamMatcher(),
            escapes={":": r"\:"},
            placeholder_style=self._placeholder_style_map[self._paramstyle],
        )
        return query_formatter
