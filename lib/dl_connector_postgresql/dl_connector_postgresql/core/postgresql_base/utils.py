from typing import (
    AbstractSet,
    Any,
    Union,
)

import sqlalchemy as sa
from sqlalchemy.engine import Dialect


def compile_pg_query(
    query: Union[str, sa.sql.ClauseElement],
    dialect: Dialect,
    exclude_types: AbstractSet[Any] = frozenset(),
    add_types: bool = True,
) -> tuple[str, list]:
    """
    Compile query
    """
    if isinstance(query, str):
        return query, []

    if isinstance(query, sa.sql.ddl.DDLElement):
        compiled = query.compile(dialect=dialect)
        return compiled.string, []

    if isinstance(query, sa.sql.ClauseElement):
        if isinstance(query, (sa.sql.dml.Insert, sa.sql.dml.Update)):
            # query = execute_defaults(query)  # default values for Insert/Update
            raise Exception("Not supported here: insert / update SA queries. Use `copy_records_to_table`.")

        compiled = query.compile(dialect=dialect, compile_kwargs={"render_postcompile": True})
        compiled_params = sorted(compiled.params.items())
        if add_types and hasattr(dialect, "dbapi"):
            pg_types = dialect.dbapi.pg_types  # type: ignore  # TODO: fix
            input_sizes = compiled._get_set_input_sizes_lookup(exclude_types=exclude_types)
        else:
            pg_types = {}
            input_sizes = {}
        items = sorted([(key, input_sizes.get(bindparam)) for bindparam, key in compiled.bind_names.items()])
        mapping = {
            key: "$%d::%s" % (idx, typ) if typ else "$%d" % idx
            for idx, (key, typ) in enumerate(
                ((key, pg_types.get(typ)) for key, typ in items),
                1,
            )
        }
        new_query: str = compiled.string % mapping

        processors = compiled._bind_processors
        new_params = [processors[key](val) if key in processors else val for key, val in compiled_params]

        return new_query, new_params

    raise Exception("Unknown query object type", type(query))
