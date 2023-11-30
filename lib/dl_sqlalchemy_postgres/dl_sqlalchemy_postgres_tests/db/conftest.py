import pytest
import sqlalchemy
import sqlalchemy.engine as sqlalchemy_engine
import sqlalchemy.orm as sqlalchemy_orm

from dl_testing.containers import get_test_container_hostport
from dl_testing.utils import wait_for_port


@pytest.fixture(scope="session")
def engine_url() -> str:
    db_postgres_hostport = get_test_container_hostport("db-postgres", fallback_port=52301)
    wait_for_port(db_postgres_hostport.host, db_postgres_hostport.port)
    return f"bi_postgresql://datalens:qwerty@{db_postgres_hostport.as_pair()}/test_data"


@pytest.fixture
def sa_engine(engine_url: str) -> sqlalchemy_engine.Engine:
    return sqlalchemy.create_engine(engine_url)


@pytest.fixture
def sa_session_maker(sa_engine) -> sqlalchemy_orm.sessionmaker:
    return sqlalchemy_orm.sessionmaker(bind=sa_engine)


@pytest.fixture
def sa_session(sa_session_maker) -> sqlalchemy_orm.Session:
    return sa_session_maker()
