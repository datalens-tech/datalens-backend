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


@attr.s(frozen=True)
class JinjaPlaceholderStyle(PlaceholderStyle):
    """Placeholder style corresponding to the "named" DBAPI paramstyle"""

    def make_placeholder(self, param_name: str, param_idx: int, user_type: UserDataType) -> str:
        return f"{{{param_name}}}"


@attr.s(frozen=True)
class JinjaQueryFormatterFactory(QueryFormatterFactory):
    def get_query_formatter(self) -> QueryFormatter:
        query_formatter = PlaceholderQueryFormatter(
            unknown_param_policy=UnknownParameterPolicy.skip,
            unconsumed_param_policy=UnconsumedParameterPolicy.ignore,
            param_matcher=JinjaStyleParamMatcher(),
            placeholder_style=JinjaPlaceholderStyle(),
        )
        return query_formatter
