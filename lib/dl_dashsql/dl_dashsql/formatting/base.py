import abc
from enum import Enum
import re
from typing import (
    Mapping,
    Sequence,
)

import attr

from dl_constants.enums import UserDataType
from dl_dashsql.types import IncomingDSQLParamTypeExt


@attr.s(frozen=True, kw_only=True)
class ParamMatcher(abc.ABC):
    """
    Class describes how to find original parameter placeholders in the query
    and how to isolate the parameter name from such placeholders.
    """

    @abc.abstractmethod
    def get_param_regex(self) -> re.Pattern:
        raise NotImplementedError

    def get_param_name(self, param_match: re.Match) -> str:
        result = param_match.group(1)  # for simple unnamed groups
        assert result is not None
        return result

    def get_original_text(self, param_match: re.Match) -> str:
        result = param_match.group(0)
        assert result is not None
        return result


@attr.s(frozen=True, kw_only=True)
class JinjaStyleParamMatcher(ParamMatcher):
    def get_param_regex(self) -> re.Pattern:
        return re.compile(r"\{\{([^}]+)\}\}")


class UnknownParameterPolicy(Enum):
    """
    Tells the formatter what to do with unknown parameters
    that appear in the query, but are not given as `raw_params`.
    """

    # Raise an error whenever an unknown parameter is discovered in the query
    error = "error"
    # Skip unknown parameters, leave their original placeholders "as-is"
    ignore = "ignore"


class UnconsumedParameterPolicy(Enum):
    """
    Tells the formatter what to do with extra parameters
    that are given as `raw_params`, but do not appear in the query.
    """

    # Raise an error if any parameters are left unconsumed
    error = "error"
    # Ignore unconsumed parameters
    ignore = "ignore"


@attr.s(frozen=True, kw_only=True)
class QueryIncomingParameter:
    original_name: str = attr.ib()
    value: IncomingDSQLParamTypeExt = attr.ib()
    user_type: UserDataType = attr.ib()


@attr.s(frozen=True, kw_only=True)
class QueryBoundParameterWrapper:
    incoming_param: QueryIncomingParameter = attr.ib()
    param_name: str = attr.ib()
    param_idx: int = attr.ib()


@attr.s(frozen=True, kw_only=True)
class FormattedQuery:
    query: str = attr.ib()
    bound_params: tuple[QueryBoundParameterWrapper, ...] = attr.ib()


@attr.s(frozen=True, kw_only=True)
class QueryFormattingResult:
    # DBAPI-compliant query
    formatted_query: FormattedQuery = attr.ib()
    # parameter names that were found in the query
    found_param_names: frozenset[str] = attr.ib()
    # parameter names that were found, but weren't known before parsing
    unknown_param_names: frozenset[str] = attr.ib()
    # parameter names that were given, but not found in the query
    unconsumed_param_names: frozenset[str] = attr.ib()


@attr.s(frozen=True, kw_only=True)
class QueryFormattingState:
    """
    A mutable structure for internal usage by formatter.
    """

    found_param_names: set[str] = attr.ib(factory=set)
    unknown_param_names: set[str] = attr.ib(factory=set)
    incoming_param_dict: Mapping[str, QueryIncomingParameter] = attr.ib()
    bound_params: list[QueryBoundParameterWrapper] = attr.ib(factory=list)
    param_name_map: dict[str, str] = attr.ib(factory=dict)

    used_param_names: set[str] = attr.ib(init=False)

    @used_param_names.default
    def _make_used_param_names(self) -> set[str]:
        return set(self.incoming_param_dict)


@attr.s(frozen=True, kw_only=True)
class QueryFormatter:
    _unknown_param_policy: UnknownParameterPolicy = attr.ib(default=UnknownParameterPolicy.error)
    _unconsumed_param_policy: UnconsumedParameterPolicy = attr.ib(default=UnconsumedParameterPolicy.ignore)
    _param_matcher: ParamMatcher = attr.ib(default=JinjaStyleParamMatcher())
    _escapes: dict[str, str] = attr.ib(factory=dict)

    @abc.abstractmethod
    def make_param_replacement(
        self,
        original_text: str,
        param_name: str,
        format_state: QueryFormattingState,
    ) -> str:
        raise NotImplementedError

    def escape_query(self, query: str) -> str:
        for orig_str, repl_str in self._escapes.items():
            query = query.replace(orig_str, repl_str)
        return query

    def format_query(self, query: str, incoming_parameters: Sequence[QueryIncomingParameter]) -> QueryFormattingResult:
        query = self.escape_query(query)

        # Initialize the formatter state
        incoming_param_dict = {param.original_name: param for param in incoming_parameters}
        format_state = QueryFormattingState(incoming_param_dict=incoming_param_dict)

        def make_repl_with_state(param_match: re.Match) -> str:
            """Closure for passing on the state object"""
            original_text = self._param_matcher.get_original_text(param_match)
            param_name = self._param_matcher.get_param_name(param_match)
            return self.make_param_replacement(
                original_text=original_text,
                param_name=param_name,
                format_state=format_state,
            )

        param_regex = self._param_matcher.get_param_regex()
        formatted_query_str = re.sub(param_regex, make_repl_with_state, query)

        unconsumed_param_names = frozenset(incoming_param_dict) - format_state.found_param_names
        if unconsumed_param_names and self._unconsumed_param_policy == UnconsumedParameterPolicy.error:
            raise RuntimeError(f"Unconsumed query parameters: {sorted(unconsumed_param_names)}")

        result = QueryFormattingResult(
            formatted_query=FormattedQuery(
                query=formatted_query_str,
                bound_params=tuple(format_state.bound_params),
            ),
            found_param_names=frozenset(format_state.found_param_names),
            unknown_param_names=frozenset(format_state.unknown_param_names),
            unconsumed_param_names=frozenset(unconsumed_param_names),
        )

        return result


@attr.s(frozen=True)
class QueryFormatterFactory(abc.ABC):
    @abc.abstractmethod
    def get_query_formatter(self) -> QueryFormatter:
        raise NotImplementedError
