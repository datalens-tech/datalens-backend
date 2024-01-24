from typing import Sequence

from dl_dashsql.formatting.base import QueryIncomingParameter
from dl_dashsql.formatting.values import DefaultValueQueryFormatterFactory


def format_query_for_debug(query: str, incoming_parameters: Sequence[QueryIncomingParameter]) -> str:
    """
    Format query with param values.
    For debug purposes only.
    """

    formatter_factory = DefaultValueQueryFormatterFactory()
    formatter = formatter_factory.get_query_formatter()
    formatting_result = formatter.format_query(query=query, incoming_parameters=incoming_parameters)
    return formatting_result.formatted_query.query
