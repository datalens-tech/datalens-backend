"""
Query formatting with usage of parameter values
"""
import abc
import datetime
from typing import Any

import attr

from dl_constants.enums import UserDataType
from dl_dashsql.formatting.base import (
    QueryFormatter,
    QueryFormatterFactory,
    QueryFormattingState,
)


@attr.s(frozen=True)
class ParamValueFormatter(abc.ABC):
    @abc.abstractmethod
    def format_value(self, value: Any, user_type: UserDataType) -> str:
        raise NotImplementedError


@attr.s(frozen=True)
class ValueQueryFormatter(QueryFormatter):
    """
    Query formatter that replaces params with their values
    """

    _value_formatter: ParamValueFormatter = attr.ib(kw_only=True)

    def make_param_replacement(
        self,
        original_text: str,
        param_name: str,
        format_state: QueryFormattingState,
    ) -> str:
        param = format_state.incoming_param_dict[param_name]
        replacement_value = self._value_formatter.format_value(value=param.value, user_type=param.user_type)
        return replacement_value


@attr.s(frozen=True)
class DefaultParamValueFormatter(ParamValueFormatter):
    def format_value(self, value: Any, user_type: UserDataType) -> str:
        if isinstance(value, datetime.date):
            return repr(value.isoformat())
        return repr(value)


@attr.s(frozen=True)
class DefaultValueQueryFormatterFactory(QueryFormatterFactory):
    def get_query_formatter(self) -> QueryFormatter:
        query_formatter = ValueQueryFormatter(value_formatter=DefaultParamValueFormatter())
        return query_formatter
