from __future__ import annotations


from dl_core.connectors.base.query_compiler import QueryCompiler


class OracleQueryCompiler(QueryCompiler):
    force_nulls_lower_than_values = True
