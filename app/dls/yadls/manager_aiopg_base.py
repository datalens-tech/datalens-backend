"""
Supporting functionality of a permissions manager over `aiopg`:
configuration;
database;
"""
from __future__ import annotations

import functools
import logging
from contextlib import asynccontextmanager
from typing import ClassVar, Type

from bi_utils.utils import DotDict

from . import db as db_base
from .db_utils import DatabaseRouting
from .node_classes import NodeWrap, SubjectWrap, SubjectGrantWrap
from .manager_base import DLSBase
from .subjects_manager import SubjectsManager


# perf-flags:
# ... ~0.128s avg
PERF_USE_SLAVE_DB = True  # -> ~0.070s ~0.063s avg
PERF_PARALLEL_PERM_CHECK = True  # -> ~0.050s ~0.045s avg
PERF_SINGLE_SQL_PERM_CHECK = True  # -> ~0.040s ~0.034s avg


def ensure_db_wrap(func, writing, tx):

    @functools.wraps(func)
    async def wrapped(self, *args, **kwargs):
        async with self.db.manage(writing=writing, tx=tx):
            return await func(self, *args, **kwargs)

    return wrapped


ensure_db_reading = functools.partial(ensure_db_wrap, writing=False, tx=False)
ensure_db_writing = functools.partial(ensure_db_wrap, writing=True, tx=False)
ensure_db_tx = functools.partial(ensure_db_wrap, writing=True, tx=True)


class DLSPGBase(DLSBase):
    """
    Some non-db-requiring pieces of DLSPG (e.g. system groups).
    """

    # models = db_base
    Data: ClassVar[Type[db_base.Data]] = db_base.Data
    Grant: ClassVar[Type[db_base.Grant]] = db_base.Grant
    Log: ClassVar[Type[db_base.Log]] = db_base.Log
    NodeConfig: ClassVar[Type[db_base.NodeConfig]] = db_base.NodeConfig
    Node: ClassVar[Type[db_base.Node]] = db_base.Node
    Subject: ClassVar[Type[db_base.Subject]] = db_base.Subject
    group_members_m2m: ClassVar[Type[db_base.group_members_m2m]] = db_base.group_members_m2m

    subjects_manager_cls = SubjectsManager

    _static_group_data_base = (
        ('id', None), ('kind', 'group'), ('source', 'system'),
    )

    def get_superuser_group(self, **kwargs):  # pylint: disable=arguments-differ
        return SubjectWrap(DotDict(
            self._static_group_data_base,
            node_identifier=str(self.SUPERUSER_GROUP_UUID),
            name=self.SUPERUSER_GROUP_NAME))

    def get_active_user_group(self, **kwargs):  # pylint: disable=arguments-differ
        return SubjectWrap(DotDict(
            self._static_group_data_base,
            node_identifier=str(self.ACTIVE_USER_GROUP_UUID),
            name=self.ACTIVE_USER_GROUP_NAME))

    def _subject_grant_from(self):
        table = (
            self.Subject.__table__
            .join(self.Grant)
            .join(self.NodeConfig))
        # table = table.join(self.Node)
        return table

    def _node_columns(self, entry=True, subject=True) -> list:
        Node = self.Node
        NodeConfig = self.NodeConfig
        Subject = self.Subject
        columns = [
            NodeConfig.id.label('node_config_id'),

            NodeConfig.node_identifier.label('identifier'),

            NodeConfig.scope,
            NodeConfig.realm,

            NodeConfig.ctime,
            NodeConfig.mtime,
            NodeConfig.meta,
        ]
        if entry:
            columns += [
                Node.id,
                # Node.scope,
                # Node.identifier,
                Node.meta.label('node_meta'),
            ]
        if subject:
            columns += [
                Subject.kind,
                Subject.name,
                Subject.active,
                Subject.source,
                Subject.meta.label('subject_meta'),
            ]
        return columns

    def _subject_grant_columns(self, detailed=True) -> list:
        Grant = self.Grant
        Subject = self.Subject
        columns = [
            # NOTE: each grant is mocked up as an annotated subject.
            Subject.id,
            Subject.kind,
            Subject.name,
            # ...
            # the id of nodeconfig that the grant is attached to:
            Grant.node_config_id,
            Grant.perm_kind,
            Grant.guid.label('grant_guid'),
            Grant.active.label('grant_active'),
            Grant.state.label('grant_state'),
        ]
        if detailed:
            columns += [
                Subject.source,
                Subject.meta,
                Subject.ctime,
                Subject.mtime,
                Grant.id.label('grant_id'),
                Grant.meta.label('grant_meta'),
                Grant.ctime.label('grant_ctime'),
                Grant.mtime.label('grant_mtime'),
                # Grant.realm,
            ]
        return columns


class DLSPGDB(DLSPGBase):
    """ DLSPGBase + db + context """

    node_cls = NodeWrap  # type: ignore  # TODO: fix
    subject_cls = SubjectWrap  # type: ignore  # TODO: fix
    subject_grant_cls = SubjectGrantWrap

    logger = logging.getLogger('DLSPG')
    context = None

    autocreate_subjects = True

    @classmethod
    @asynccontextmanager
    async def create_default(cls, manage=True, writing=True, tx=True, **kwargs):
        from . import db
        async with await db.get_engine_aiopg() as db_pool:
            mgr = cls(
                db_cfg=dict(db_pool_writing=db_pool, db_pool_reading=db_pool),
                **kwargs,
            )
            if not manage:
                yield mgr
            else:
                async with mgr.db.manage(writing=writing, tx=tx):
                    yield mgr

    def __init__(self, db_cfg, context=None, _db_router=None, **kwargs):
        self._kwargs = dict(kwargs, db_cfg=db_cfg)

        if context is None:
            context = {}
        self.context = context

        if not PERF_USE_SLAVE_DB:
            db_cfg = dict(db_pool_writing=db_cfg['db_pool_writing'], db_pool_reading=db_cfg['db_pool_writing'])
        if _db_router is None:
            _db_router = DatabaseRouting(**db_cfg)
        else:
            assert all(val is getattr(_db_router, key) for key, val in db_cfg.items())
        self.db = _db_router
        super().__init__(**kwargs)

    def clone(self, pass_conn=False, pass_context=True):
        kwargs = self._kwargs
        if pass_conn:
            kwargs = dict(kwargs, _db_router=self.db)
        if pass_context:
            kwargs = dict(kwargs, context=self.context.copy())  # type: ignore  # TODO: fix
        return self.__class__(**kwargs)

    @property
    def realm(self) -> str:
        ctx = self.context or {}
        realm = ctx.get("realm")
        if realm:
            return realm
        tenant_info = ctx.get("tenant_info")
        if tenant_info:
            folder_id = tenant_info.get("folder_id")
            if folder_id:
                return folder_id
            org_id = tenant_info.get("org_id")
            if org_id:
                return f"org_{org_id}"
        return ""

    @property
    def db_conn(self):
        return self.db.conn
