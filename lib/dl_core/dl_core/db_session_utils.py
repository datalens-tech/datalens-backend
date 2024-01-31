# TODO FIX: Move this file to connection_executor package after migration to connection executors will be done

from __future__ import annotations

from contextlib import contextmanager
import logging
from typing import (
    Collection,
    Generator,
    Type,
)

from sqlalchemy.engine import Engine
import sqlalchemy.exc
from sqlalchemy.orm import (
    Query,
    Session,
    sessionmaker,
)

from dl_app_tools.profiling_base import GenericProfiler
from dl_constants.enums import SourceBackendType
from dl_core import exc


LOGGER = logging.getLogger(__name__)


_SA_QUERY_CLASSES: dict[SourceBackendType, Type[Query]] = {}


def register_sa_query_cls(backend_type: SourceBackendType, query_cls: Type[Query]) -> None:
    if backend_type in _SA_QUERY_CLASSES:
        assert _SA_QUERY_CLASSES[backend_type] is query_cls
    else:
        _SA_QUERY_CLASSES[backend_type] = query_cls


def get_sa_query_cls(backend_type: SourceBackendType) -> Type[Query]:
    return _SA_QUERY_CLASSES[backend_type]


class CustomSession(Session):
    __closed = False

    def close(self):  # type: ignore  # 2024-01-30 # TODO: Function is missing a return type annotation  [no-untyped-def]
        super().close()
        self.__closed = True

    def execute(self, *args, **kwargs):  # type: ignore  # 2024-01-30 # TODO: Function is missing a type annotation  [no-untyped-def]
        if self.__closed:
            raise exc.DBSessionError("Cannot execute on closed session")
        return super().execute(*args, **kwargs)


def get_db_session(db_engine: Engine, backend_type: SourceBackendType) -> Session:
    session_kwargs = {}
    session_kwargs["query_cls"] = get_sa_query_cls(backend_type=backend_type)
    SessionCls = sessionmaker(bind=db_engine, class_=CustomSession)
    return SessionCls(**session_kwargs)


_QUERY_FAIL_EXCEPTIONS: set[Type[Exception]] = {
    sqlalchemy.exc.InvalidRequestError,
    sqlalchemy.exc.DBAPIError,
}


def register_query_fail_exceptions(exception_classes: Collection[Type[Exception]]) -> None:
    _QUERY_FAIL_EXCEPTIONS.update(exception_classes)


def get_query_fail_exceptions() -> tuple[Type[Exception], ...]:
    return tuple(_QUERY_FAIL_EXCEPTIONS)


@contextmanager
def db_session_context(db_engine: Engine, backend_type: SourceBackendType) -> Generator[Session, None, None]:
    LOGGER.info("Going to create DB session")
    with GenericProfiler("db-create-session"):
        session = get_db_session(db_engine=db_engine, backend_type=backend_type)

    try:
        yield session
    except Exception as err:
        try:
            session.rollback()
        except get_query_fail_exceptions():
            LOGGER.info(
                "Rollback failed because transaction has already been rolled back " "due to an internal exception"
            )
        LOGGER.error("Rolling back transaction because of error: %s", err)
        raise
    else:
        session.commit()
    finally:
        session.close()
        db_engine.dispose()
