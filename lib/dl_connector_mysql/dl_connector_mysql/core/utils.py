import datetime
from typing import Union

import sqlalchemy as sa
from sqlalchemy import sql as sasql
from sqlalchemy.engine import Dialect


def compile_mysql_query(
    query: Union[str, sasql.Select],
    dialect: Dialect,
    escape_percent: bool,
) -> tuple[str, dict]:
    """
    Compile query
    """
    if isinstance(query, str):
        return query, {}

    compiled = query.compile(dialect=dialect, compile_kwargs={"render_postcompile": True})

    # same logic as in DLMYSQLCompilerBasic
    type_to_literal = {
        sa.Date: "DATE",
        sa.DateTime: "TIMESTAMP",
    }

    value_converters = {
        datetime.date: lambda d: d.isoformat(),
        datetime.datetime: lambda dt: dt.replace(tzinfo=None).isoformat(),
        str: lambda s: s.removesuffix("+00:00"),
    }

    templates_and_values = {}
    for name, value in compiled.params.items():
        type_ = compiled.binds[name].type
        for type_key, type_literal in type_to_literal.items():
            if isinstance(type_, type_key):
                for value_type, value_conv in value_converters.items():
                    if isinstance(value, value_type):
                        templates_and_values[name] = (f"{type_literal}%({name})s", value_conv(value))
        templates_and_values.setdefault(name, (f"%({name})s", value))

    new_query: str = compiled.string
    if escape_percent:
        new_query = new_query.replace("%%", "%%%%")
    new_query = new_query % {key: tv[0] for key, tv in templates_and_values.items()}
    new_params = {key: tv[1] for key, tv in templates_and_values.items()}
    return new_query, new_params
