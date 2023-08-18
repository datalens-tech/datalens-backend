from __future__ import annotations

from sqlalchemy.ext.declarative import declarative_base


class Base(declarative_base()):  # type: ignore  # TODO: fix

    __abstract__ = True

    @classmethod
    def select_(cls, whereclause=None, **params):  # type: ignore  # TODO: fix
        return cls.__table__.select(whereclause=whereclause, **params)

    @classmethod
    def insert_(cls, **params):  # type: ignore  # TODO: fix
        return cls.__table__.insert(**params)

    @classmethod
    def delete_(cls, *args, **params):  # type: ignore  # TODO: fix
        return cls.__table__.delete(*args, **params)

    @classmethod
    def update_(cls, *args, **params):  # type: ignore  # TODO: fix
        return cls.__table__.update(*args, **params)
