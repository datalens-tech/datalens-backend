"""
See also: `./cache_manager_notes.md`
"""
from __future__ import annotations

import time
import logging
from typing import Optional, Sequence

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sa_pg

from .utils import chunks
from . import db as db_base
from . import db_utils
from . import db_walkers


LOG = logging.getLogger(__name__)


class CacheManager:

    prefix: str
    bulk_select_sql: str

    db_get_engine = staticmethod(db_base.get_engine)
    Data = db_base.Data
    update_chunk_size = 100
    delete_chunk_size = db_base.DEFAULT_DELETE_CHUNK_SIZE
    db_engine = None

    def __init__(self):
        self.logger = LOG.getChild(self.__class__.__name__)

    @classmethod
    def bulk_update_stmt(cls, keys):
        columns = ('key', 'data', 'meta')
        table = cls.Data.__table__
        stmt = sa.text(cls.bulk_select_sql)
        stmt = stmt.bindparams(prefix=cls.prefix, keys=tuple(keys))
        stmt = stmt.columns(key=sa.String, data=sa.String, meta=sa_pg.JSONB)
        stmt = sa_pg.insert(table).from_select(columns, stmt)
        stmt = db_utils.bulk_upsert_for_insert_statement(
            stmt, columns=('meta',), key=('key',), auto_where=True)
        return stmt

    def update_cache_bulk(self, keys):
        db_engine = self.db_engine
        if db_engine is None:
            db_engine = self.db_get_engine()
        keys = sorted(keys)
        for chunk in chunks(keys, self.update_chunk_size):
            self.logger.debug("Updating for %r .. %r", chunk[0], chunk[-1])
            stmt = self.bulk_update_stmt(chunk)
            db_engine.execute(stmt)

    def delete_cache(self, keys: Sequence[str]):
        db_engine = self.db_engine
        if db_engine is None:
            db_engine = self.db_get_engine()

        model = self.Data

        for chunk in chunks(keys, self.delete_chunk_size):
            chunk = tuple('{}{}'.format(self.prefix, key) for key in chunk)
            stmt = model.delete_(model.key.in_(chunk))
            db_engine.execute(stmt)


BULK_UPDATE_EFFECTIVE_GROUPS_SELECT_SQL = """
select
  :prefix || "user"."name" as key,
  '' as data,
  json_build_object(
    'groups', jsonb_agg(
      json_build_object('name', "group"."name")
    )
  ) as meta
  from (
{}
    ) membership
    join dls_subject "user" on membership.user_id = "user".id
    join dls_subject "group" on membership.group_id = "group".id
  group by "user"."name"
""".format(
    db_walkers.sql_effective_groups_bulk(
        target_clause='(select id from dls_subject where name in :keys)'))


class EffectiveGroupsCacheManager(CacheManager):

    prefix: str = 'cache__effective_groups__'
    bulk_select_sql: str = BULK_UPDATE_EFFECTIVE_GROUPS_SELECT_SQL
    update_chunk_size = 100

    def main(self, subjects_names: Sequence[str], obsolete_subjects_names: Optional[Sequence[str]] = None) -> None:
        LOG.info("update_cache_bulk(<%d subjects>)...", len(subjects_names))
        t1 = time.time()
        self.update_cache_bulk(subjects_names)
        t2 = time.time()
        LOG.info(
            "update_search_data(<%d subjects>) done in %.3fs (%.5fs/item).",
            len(subjects_names), t2 - t1, (t2 - t1) / len(subjects_names))
        if obsolete_subjects_names is not None:
            LOG.info("delete_cache(<%d subjects>)...", len(obsolete_subjects_names))
            t1 = time.time()
            self.delete_cache(obsolete_subjects_names)
            t2 = time.time()
            LOG.info(
                "delete_cache(<%d subjects>) done in %.3fs (%.5fs/item).",
                len(obsolete_subjects_names),
                t2 - t1,
                (t2 - t1) / len(obsolete_subjects_names))
