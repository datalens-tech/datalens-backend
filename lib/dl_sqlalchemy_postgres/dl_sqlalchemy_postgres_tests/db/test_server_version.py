import mock
import sqlalchemy
import sqlalchemy.dialects.postgresql.psycopg2 as sqlalchemy_dialect_psycopg2
import sqlalchemy.orm as sqlalchemy_orm


SERVER_VERSION_INFO = (123, 45, 67, 89)
SERVER_VERSION = ".".join(map(str, SERVER_VERSION_INFO))


@mock.patch.object(sqlalchemy_dialect_psycopg2.PGDialect_psycopg2, "_get_server_version_info")
def test_server_version_default(patched_server_version: mock.Mock, sa_session: sqlalchemy_orm.Session):
    patched_server_version.return_value = SERVER_VERSION_INFO

    sa_session.scalar("select 1")

    assert sa_session.get_bind().dialect.server_version_info == SERVER_VERSION_INFO
    patched_server_version.assert_called_once()


@mock.patch.object(sqlalchemy_dialect_psycopg2.PGDialect_psycopg2, "_get_server_version_info")
def test_server_version_error(patched_server_version: mock.Mock, sa_session: sqlalchemy_orm.Session):
    patched_server_version.side_effect = AssertionError

    sa_session.scalar("select 1")

    assert sa_session.get_bind().dialect.server_version_info == (9, 3)
    patched_server_version.assert_called_once()


@mock.patch.object(sqlalchemy_dialect_psycopg2.PGDialect_psycopg2, "_get_server_version_info")
def test_server_version_overwritten(patched_server_version: mock.Mock, engine_url: str):
    sa_engine = sqlalchemy.create_engine(engine_url, connect_args=dict(server_version=SERVER_VERSION))
    sa_session_maker = sqlalchemy_orm.sessionmaker(bind=sa_engine)
    sa_session = sa_session_maker()

    sa_session.scalar("select 1")

    assert sa_session.get_bind().dialect.server_version_info == SERVER_VERSION_INFO
    patched_server_version.assert_not_called()
