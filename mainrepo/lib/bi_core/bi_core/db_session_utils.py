# TODO FIX: Move this file to connection_executor package after migration to connection executors will be done

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Collection, Generator, Type

import sqlalchemy.exc
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session, Query

from bi_constants.enums import SourceBackendType

from bi_core import exc
from bi_app_tools.profiling_base import GenericProfiler

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

    def close(self):  # type: ignore
        super().close()
        self.__closed = True

    def execute(self, *args, **kwargs):  # type: ignore
        if self.__closed:
            raise exc.DBSessionError('Cannot execute on closed session')
        return super().execute(*args, **kwargs)


def get_db_session(db_engine: Engine, backend_type: SourceBackendType) -> Session:
    session_kwargs = {}
    session_kwargs['query_cls'] = get_sa_query_cls(backend_type=backend_type)
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
                'Rollback failed because transaction has already been rolled back '
                'due to an internal exception')
        LOGGER.error('Rolling back transaction because of error: %s', err)
        raise
    else:
        session.commit()
    finally:
        session.close()
        db_engine.dispose()
