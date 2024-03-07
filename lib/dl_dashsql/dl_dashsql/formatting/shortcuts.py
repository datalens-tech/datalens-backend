from typing import Sequence

from dl_dashsql.formatting.base import QueryIncomingParameter
from dl_dashsql.formatting.values import DefaultValueQueryFormatterFactory
from dl_dashsql.typed_query.primitives import TypedQueryParameter


def params_for_formatter(params: Sequence[TypedQueryParameter]) -> Sequence[QueryIncomingParameter]:
    return [
        QueryIncomingParameter(
            original_name=param.name,
            user_type=param.typed_value.type,
            value=param.typed_value.value,
        )
        for param in params
    ]


def format_query_for_debug(query: str, incoming_parameters: Sequence[QueryIncomingParameter]) -> str:
    """
    Format query with param values.
    For debug purposes only.
    """

    formatter_factory = DefaultValueQueryFormatterFactory()
    formatter = formatter_factory.get_query_formatter()
    formatting_result = formatter.format_query(query=query, incoming_parameters=incoming_parameters)
    return formatting_result.formatted_query.query
