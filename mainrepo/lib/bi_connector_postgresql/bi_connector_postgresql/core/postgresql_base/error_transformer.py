import asyncpg.exceptions as asyncpg_exc
import psycopg2.errors

from bi_core.connectors.base.error_transformer import (
    ChainedDbErrorTransformer,
    DbErrorTransformer,
)
from bi_core.connectors.base.error_transformer import (
    make_rule_from_descr,
    orig_exc_is,
    wrapper_exc_is_and_matches_re,
)
from bi_core.connectors.base.error_transformer import ErrorTransformerRule as Rule
import bi_core.exc as exc

from bi_connector_postgresql.core.postgresql_base.exc import (
    PgDoublePrecisionRoundError,
    PostgresSourceDoesNotExistError,
)

sync_pg_db_error_transformer: DbErrorTransformer = ChainedDbErrorTransformer(
    [
        Rule(
            when=wrapper_exc_is_and_matches_re(
                wrapper_exc_cls=psycopg2.OperationalError, err_regex_str="Name or service not known"
            ),
            then_raise=exc.SourceHostNotKnownError,
        ),
        Rule(
            when=orig_exc_is(orig_exc_cls=psycopg2.errors.UndefinedTable),  # type: ignore
            then_raise=PostgresSourceDoesNotExistError,
        ),
    ]
)


def make_async_pg_error_transformer() -> DbErrorTransformer:
    rule_descriptions = (
        ((asyncpg_exc.DivisionByZeroError, None), exc.DivisionByZero),
        (
            (asyncpg_exc.UndefinedFunctionError, r"function round\(double precision"),
            PgDoublePrecisionRoundError,
        ),
        ((asyncpg_exc.UndefinedFunctionError, None), exc.UnknownFunction),
        ((OverflowError, "value out of .* range"), exc.NumberOutOfRange),
        ((asyncpg_exc.InvalidTextRepresentationError, None), exc.DataParseError),
        # ((asyncpg_exc.UndefinedTableError, None), exc.SourceDoesNotExist),
        ((asyncpg_exc.UndefinedTableError, None), PostgresSourceDoesNotExistError),
        # ((asyncpg_exc.FDWTableNotFoundError, None), exc.SourceDoesNotExist),
        ((asyncpg_exc.FDWTableNotFoundError, None), PostgresSourceDoesNotExistError),  # todo: test for this error
        ((asyncpg_exc.SyntaxOrAccessError, None), exc.DatabaseOperationalError),
        ((TimeoutError, None), exc.SourceConnectError),
        ((OSError, "Name or service not known"), exc.SourceHostNotKnownError),
    )
    return ChainedDbErrorTransformer([make_rule_from_descr(d) for d in rule_descriptions])
