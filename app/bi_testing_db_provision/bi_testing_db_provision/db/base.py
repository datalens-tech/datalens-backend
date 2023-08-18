from __future__ import annotations

from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base

convention = {
    'all_column_names': lambda constraint, table: '_'.join([
        column.name for column in constraint.columns.values()
    ]),
    'ix': 'ix__%(table_name)s__%(all_column_names)s',
    'uq': 'uq__%(table_name)s__%(all_column_names)s',
    'ck': 'ck__%(table_name)s__%(constraint_name)s',
    'fk': 'fk__%(table_name)s__%(all_column_names)s__%(referred_table_name)s',
    'pk': 'pk__%(table_name)s'
}


class Base(declarative_base(metadata=MetaData(naming_convention=convention))):  # type: ignore  # TODO: fix
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
