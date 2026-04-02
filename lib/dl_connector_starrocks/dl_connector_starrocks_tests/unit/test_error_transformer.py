import pymysql
import sqlalchemy.exc

from dl_core.exc import (
    InvalidQuery,
    SourceDoesNotExist,
)

from dl_connector_starrocks.core.error_transformer import (
    async_starrocks_db_error_transformer,
    sync_starrocks_db_error_transformer,
)


def test_table_does_not_exist_async() -> None:
    transformer = async_starrocks_db_error_transformer

    wrapper_exc = pymysql.OperationalError(1051, "Unknown table 'test_db.test_table'")
    parameters = transformer.make_bi_error_parameters(wrapper_exc=wrapper_exc)

    assert parameters[0] == SourceDoesNotExist


def test_table_does_not_exist_sync() -> None:
    transformer = sync_starrocks_db_error_transformer

    orig = pymysql.OperationalError(1051, "Unknown table 'test_db.test_table'")
    wrapper_exc = sqlalchemy.exc.OperationalError(
        statement="SELECT 1",
        params={},
        orig=orig,
    )
    parameters = transformer.make_bi_error_parameters(wrapper_exc=wrapper_exc)

    assert parameters[0] == SourceDoesNotExist


def test_sql_syntax_error_async() -> None:
    transformer = async_starrocks_db_error_transformer

    wrapper_exc = pymysql.ProgrammingError(1064, "You have an error in your SQL syntax")
    parameters = transformer.make_bi_error_parameters(wrapper_exc=wrapper_exc)

    assert parameters[0] == InvalidQuery


def test_sql_syntax_error_sync() -> None:
    transformer = sync_starrocks_db_error_transformer

    orig = pymysql.ProgrammingError(1064, "You have an error in your SQL syntax")
    wrapper_exc = sqlalchemy.exc.ProgrammingError(
        statement="SELECT !!!",
        params={},
        orig=orig,
    )
    parameters = transformer.make_bi_error_parameters(wrapper_exc=wrapper_exc)

    assert parameters[0] == InvalidQuery
