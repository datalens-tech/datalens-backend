"""
== Alembic usage ==

Useful state commands:

    alembic current -v
    alembic history -v
    alembic heads -v


Run migrations: `alembic upgrade head`

Rollback last migration: `alembic downgrade -1`
http://alembic.zzzcomputing.com/en/latest/tutorial.html#relative-migration-identifiers

Rollback to nothing: `alembic downgrade base`,
therefore, to reset db: `alembic downgrade base && alembic upgrade head`.

Auto-generate migrations from the current db state.
WARNING: often require manual edits.

    alembic revision --autogenerate -m <message>
    git add alembic/versions/*.py
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Optional, cast

import sqlalchemy as sa
import sqlalchemy.orm as sa_orm
import sqlalchemy.ext.declarative as sa_declarative
import sqlalchemy.dialects.postgresql as sa_pg
from sqlalchemy.orm.exc import NoResultFound  # pylint: disable=unused-import

if TYPE_CHECKING:
    import aiopg.sa


__all__ = (
    'NoResultFound',
    'BULK_ENTITY_LIMIT',
    'DEFAULT_INSERT_CHUNK_SIZE',
    'DEFAULT_DELETE_CHUNK_SIZE',
    'get_cfg',
    'get_cfg_slave',
    'dbg_dbshell',
    'get_engine',
    'get_engine_aiopg',
    'get_session',
    'subject_kind',
    'Node',
    'NodeConfig',
    'Subject',
    'group_members_m2m',
    'subject_memberships_m2m',
    'Grant',
    'Log',
    'Data',
    'PeriodicTask',
    'SSWord',
    'ss_word_subjects_m2m',
)


TDBEngine = sa.engine.base.Engine
TDBSession = sa_orm.session.Session


# Do not ever try to load more than this much into memory.
BULK_ENTITY_LIMIT = 128000


LOG = logging.getLogger(__name__)
DEFAULT_INSERT_CHUNK_SIZE = 8000
DEFAULT_DELETE_CHUNK_SIZE = 2000


def get_cfg() -> dict:
    from .settings import settings
    cfg = dict(
        host=settings.DB_HOST,
        port=int(settings.DB_PORT),
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        database=settings.DB_NAME,
        target_session_attrs='read-write',

        connect_timeout=0.9,
        # sslmode='require',

        sslmode='verify-full',
        # `yandex-internal-root-ca` deb-package.
        sslrootcert='/usr/share/yandex-internal-root-ca/YandexInternalRootCA.crt',

        # sslrootcert='/etc/ssl/certs/ca-certificates.crt',
    )
    # TODO: some defaults:
    #     application_name='dls_etc',
    #     connect_timeout=2,
    conn_str = "{user}@{host}:{port}/{database}".format(**cfg)
    LOG.info("Using database: %s", conn_str)
    if settings.DBG_DBSHELL == 'default':
        dbg_dbshell('default', cfg)
    return cfg


def get_cfg_slave() -> dict:
    from .settings import settings
    result = dict(get_cfg())
    overrides = dict(
        host=settings.DB_SLAVE_HOST,
        port=settings.DB_SLAVE_PORT,
        user=settings.DB_SLAVE_USER,
        password=settings.DB_SLAVE_PASSWORD,
        database=settings.DB_SLAVE_NAME,
        target_session_attrs='any',
    )

    # # pgaas default
    # overrides['database'] = (
    #     overrides['database'] or
    #     '{}_ro_local'.format(result['database']))

    for key, value in overrides.items():
        if value is not None:
            result[key] = value
    if settings.DBG_DBSHELL in ('slave', 'local'):
        dbg_dbshell('slave', result)
    return result


def dbg_dbshell(name: str, cfg: dict) -> None:
    import shlex
    connopts = dict(
        dbname=cfg['database'],
        user=cfg['user'],
        host=cfg['host'],
        port=cfg['port'],
    )
    connstr = " ".join("%s=%s" % (key, val) for key, val in sorted(connopts.items()))
    result = "\n".join((
        "# Manual dbshell for %s:" % (shlex.quote(name),),
        "PGPASSWORD=%s psql %s" % (
            shlex.quote(cfg['password']),
            shlex.quote(connstr)),
        "",
    ))
    print(result)


def get_engine(cfg: Optional[dict] = None, **kwargs: Any) -> TDBEngine:
    if cfg is None:
        cfg = get_cfg()
    conn_str = (
        'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
        '?target_session_attrs={target_session_attrs}'
        '&application_name=dls_sq').format(
            **cfg)
    return sa.create_engine(conn_str, **kwargs)


async def get_engine_aiopg(
        cfg: Optional[dict] = None,
        echo: bool = False, minsize: int = 3, maxsize: int = 15, timeout: int = 6,
        **kwargs: Any) -> aiopg.sa.engine.Engine:
    from aiopg.sa import create_engine

    if cfg is None:
        cfg = get_cfg()

    defaults = dict(
        echo=echo, minsize=minsize, maxsize=maxsize, timeout=timeout,
    )
    kwargs = {**defaults, **cfg, **kwargs}
    return await create_engine(**kwargs)


def get_session(engine: Optional[TDBEngine] = None, **kwargs: Any) -> TDBSession:
    if engine is None:
        engine = get_engine()
    sm = sa_orm.sessionmaker(
        autocommit=False, autoflush=False,
        bind=engine, **kwargs)
    return cast(TDBSession, sa_orm.scoped_session(sm))


class Base_Pre:
    """ Something something declarative mixin """

    # # Automatic tablename example:
    # @sa_declarative.declared_attr
    # def __tablename__(cls):
    #     return 'dls_{}'.format(cls.__name__.lower())


Base_Base = sa_declarative.declarative_base(cls=Base_Pre)
# _session = get_session()
# Base_Base._query = _session.query_property()


class Base(Base_Base):  # type: ignore  # TODO: fix

    __abstract__ = True

    @classmethod
    def select_(cls, whereclause=None, **params):
        return cls.__table__.select(whereclause=whereclause, **params)

    @classmethod
    def insert_(cls, **params):
        return cls.__table__.insert(**params)

    @classmethod
    def delete_(cls, *args, **params):
        return cls.__table__.delete(*args, **params)

    @classmethod
    def update_(cls, *args, **params):
        return cls.__table__.update(*args, **params)


def _pk_col(**kwargs):
    kwargs = {**dict(
        type_=sa.Integer, primary_key=True, autoincrement=True, nullable=False,
    ), **kwargs}
    return sa.Column(**kwargs)


def _serial_col(**kwargs):
    """ An autoincrement column for convenience and for django compatibility """
    # WARNING: this does not cause sqlalchemy to generate a 'SERIAL'-type
    # column, as it only does that for `primary_key=True` cases. Manually
    # altered migration code is required.
    kwargs = {**dict(
        type_=sa.Integer().evaluates_none(), autoincrement=True, nullable=False,
    ), **kwargs}
    return sa.Column(**kwargs)


def _timestamp_col(precision=6, timezone=True, **kwargs):
    kwargs = {**dict(
        type_=sa_pg.TIMESTAMP(precision=precision, timezone=timezone),
    ), **kwargs}
    return sa.Column(**kwargs)


def _ctime_col(**kwargs):
    kwargs = {**dict(
        server_default=sa.func.now(), index=True,
    ), **kwargs}
    return _timestamp_col(**kwargs)


def _mtime_col(**kwargs):
    kwargs = {**dict(
        server_default=sa.func.now(), index=True,
        onupdate=sa.func.current_timestamp(),
    ), **kwargs}
    return _timestamp_col(**kwargs)


def _meta_col(**kwargs):
    kwargs = {**dict(
        type_=sa_pg.JSONB, server_default="'{}'", nullable=False,
    ), **kwargs}
    return sa.Column(**kwargs)


def _realm_col(**kwargs):
    kwargs = {**dict(
        type_=sa.String, server_default="''", nullable=False, index=True,
    ), **kwargs}
    return sa.Column(**kwargs)


subject_kind = sa_pg.ENUM(
    'user', 'group', 'other',
    name='dls_subject_kind', metadata=Base.metadata)


class Node(Base):
    """ Shared node state. A 'communication' table. Only one system should write to it. """
    __tablename__ = 'dls_nodes'

    id = _pk_col()

    identifier = sa.Column(sa.String, index=True, nullable=False, unique=True)

    scope = sa.Column(sa.String, index=True, nullable=False)
    meta = _meta_col()

    # path_lvl = sa.Column(sa.Integer, index=True, nullable=True)
    # path = sa.Column(sa.String, index=True, nullable=True)

    ctime = _ctime_col()
    # mtime = _mtime_col()
    realm = _realm_col()

    # node_config = sa_orm.relationship('NodeConfig', uselist=False, backref='node')


class CommonBase(Base):
    __abstract__ = True

    id = _pk_col()
    meta = _meta_col()
    ctime = _ctime_col()
    mtime = _mtime_col()
    realm = _realm_col()

    # TODO?: https://stackoverflow.com/a/13979333


class NodeConfig(CommonBase):
    """
    Non-shared node state, e.g. current permissions.

    Additionally applies to subjects, which is why `node` is nullable.
    """
    __tablename__ = 'dls_node_config'

    # 1. Denormalized for debug convenience:
    # 2. Additionally relevant for `Subject` configs.
    node_identifier = sa.Column(sa.String, index=True, nullable=False, unique=True)
    scope = sa.Column(sa.String, index=True, nullable=False, server_default="''")

    # ...
    node_id = sa.Column(
        sa.Integer, sa.ForeignKey('dls_nodes.id', name='fk_dls_nodes_id'),
        index=True, nullable=True)
    node = sa_orm.relationship(
        Node,
        backref=sa_orm.backref("node_config", uselist=False))

    # # TODO?:
    # subject_id = sa.Column(
    #     sa.Integer, sa.ForeignKey('dls_subject.id', name='fk_dls_subject_id),
    #     index=True, nullable=True)

    # # Implicit:
    # subject = sa_orm.relationship('Subject', uselist=False, backref='node_config')


class Subject(CommonBase):
    """ Users and Groups """
    __tablename__ = 'dls_subject'

    # user|group|other
    kind = sa.Column(subject_kind, index=True, nullable=False)

    name = sa.Column(sa.String, index=True, nullable=False, unique=True)
    # path = sa.Column(sa.String, index=True, nullable=True)

    node_config_id = sa.Column(
        sa.Integer, sa.ForeignKey(NodeConfig.id, name='fk_node_config_id'),
        index=True, nullable=False)
    node_config = sa_orm.relationship(NodeConfig, backref=sa_orm.backref("subject", uselist=False))

    active = sa.Column(sa.Boolean, server_default="true", index=True, nullable=False)
    source = sa.Column(sa.String, index=True, nullable=False, server_default="'unknown'")

    search_weight = sa.Column(
        sa.Integer, index=True, nullable=False, server_default="0")
    # members = ...
    # memberships_direct = ...
    # memberships = ...  # recursive, unused
    # members_recursive = ...  # recursive, unused


# # Based on:
# http://docs.sqlalchemy.org/en/latest/_modules/examples/graphs/directed_graph.html

class group_members_m2m(Base):
    __tablename__ = 'dls_group_members_m2m'

    id = _serial_col()

    group_id = sa.Column(
        sa.Integer, sa.ForeignKey('dls_subject.id', name='fk_group_subject_id'),
        primary_key=True)
    member_id = sa.Column(
        sa.Integer, sa.ForeignKey('dls_subject.id', name='fk_member_subject_id'),
        primary_key=True)

    group = sa_orm.relationship(
        Subject, primaryjoin=group_id == Subject.id,
        backref='members')
    member = sa_orm.relationship(
        Subject, primaryjoin=member_id == Subject.id,
        backref='memberships_direct')

    source = sa.Column(sa.String, index=True, nullable=False, server_default="'unknown'")
    meta = _meta_col()
    ctime = _ctime_col()
    mtime = _mtime_col()
    realm = _realm_col()


class subject_memberships_m2m(Base):
    __tablename__ = 'dls_subject_memberships_m2m'

    id = _serial_col()

    subject_id = sa.Column(
        sa.Integer, sa.ForeignKey('dls_subject.id', name='fk_subject_id'),
        primary_key=True)
    group_id = sa.Column(
        sa.Integer, sa.ForeignKey('dls_subject.id', name='fk_group_subject_id'),
        primary_key=True)
    realm = _realm_col()

    subject = sa_orm.relationship(
        Subject, primaryjoin=subject_id == Subject.id,
        backref='memberships')
    group = sa_orm.relationship(
        Subject, primaryjoin=group_id == Subject.id,
        backref='members_recursive')


class Grant(CommonBase):
    """
    Relates the NodeConfig and the full set of subjects involved in permissions
    to it (but without the details).

    Formerly known as `node_permissions_m2m`
    """
    __tablename__ = 'dls_grant'

    __table_args__ = (
        sa.UniqueConstraint('node_config_id', 'subject_id', 'perm_kind', name='_grant_locator'),
    )

    guid = sa.Column(sa_pg.UUID, unique=True, index=True)

    perm_kind = sa.Column(sa.String, nullable=False, index=True)

    node_config_id = sa.Column(
        sa.Integer, sa.ForeignKey(NodeConfig.id, name='fk_node_config_id'),
        index=True)
    subject_id = sa.Column(
        sa.Integer, sa.ForeignKey(Subject.id, name='fk_subject_id'),
        index=True)

    node_config = sa_orm.relationship(
        NodeConfig, primaryjoin=node_config_id == NodeConfig.id,
        backref='permissions_subjects')
    subject = sa_orm.relationship(
        Subject, primaryjoin=subject_id == Subject.id,
        backref='node_permissions')

    active = sa.Column(sa.Boolean, index=True, nullable=False)
    # default | requested | ...
    state = sa.Column(sa.String, server_default="'default'", index=True, nullable=False)


class Log(CommonBase):
    """
    Changes history, for audit / offloading / etc.

    `sublocator` values:
      * `grant:{grant_guid}`
      * `group:{subject_id}`
      * ...
    """
    __tablename__ = 'dls_log'

    kind = sa.Column(sa.String, server_default="'etc'", index=True, nullable=False)
    sublocator = sa.Column(sa.String, server_default="''", index=True, nullable=False)

    grant_guid = sa.Column(
        sa_pg.UUID, sa.ForeignKey(Grant.guid, name='fk_grant_guid'),
        index=True, nullable=True)
    grant = sa_orm.relationship(
        Grant, primaryjoin=grant_guid == Grant.guid,
        backref='logs')

    # Additional filterables:
    request_user_id = sa.Column(
        sa.Integer, sa.ForeignKey(Subject.id, name='fk_request_user_subject_id'),
        index=True, nullable=True)
    request_user = sa_orm.relationship(
        Subject, primaryjoin=request_user_id == Subject.id,
        backref='logs_as_request_user')

    # Convenience, should be a copy of the `sublocator` data.
    node_identifier = sa.Column(
        sa.String, sa.ForeignKey(NodeConfig.node_identifier, name='fk_node_identifier'),
        index=True, nullable=True)
    node_config = sa_orm.relationship(
        NodeConfig, primaryjoin=node_identifier == NodeConfig.node_identifier,
        backref='logs')
    # A viewonly convenience-relationship.
    node = sa_orm.relationship(
        Node, primaryjoin=node_identifier == Node.identifier, foreign_keys=[node_identifier],
        backref='logs',
        viewonly=True,
    )


class Data(CommonBase):
    """ A common use key-value storage """
    __tablename__ = 'dls_data'

    key = sa.Column(sa.String, index=True, nullable=False, unique=True)
    data = sa.Column(sa.String, nullable=False)


class PeriodicTask(Base):
    __tablename__ = 'dls_periodic_task'

    id = _serial_col()

    name = sa.Column(sa.String, primary_key=True)

    # settings
    frequency = sa.Column(sa.Float())
    lock_expire = sa.Column(sa.Float())
    lock_renew = sa.Column(sa.Float())

    # state
    lock = sa.Column(sa.String, nullable=True)
    last_start_ts = _timestamp_col(nullable=True)
    last_success_ts = _timestamp_col(nullable=True)
    last_failure_ts = _timestamp_col(nullable=True)
    last_ping_ts = _timestamp_col(nullable=True)
    last_ping_meta = _meta_col()

    meta = _meta_col()
    realm = _realm_col()


class SSWord(Base):
    """ Subject Suggest words table """
    __tablename__ = 'dls_ss_word'

    id = _pk_col()
    word = sa.Column(sa.String, index=True, nullable=False, unique=True)


class ss_word_subjects_m2m(Base):
    __tablename__ = 'dls_ss_word_subjects'

    id = _serial_col()

    ssword_id = sa.Column(
        sa.Integer, sa.ForeignKey(SSWord.id, name='fk_ssword_id'),
        primary_key=True, index=True)
    subject_id = sa.Column(
        sa.Integer, sa.ForeignKey(Subject.id, name='fk_subject_id'),
        primary_key=True, index=True)
    realm = _realm_col()
