# coding: utf8
"""
psql -d yadls -c "select group_id || ' -> ' || member_id || ';' from dls_group_members_m2m;" | \
  sed '/^[^ ]/ d; 1 s/.*/digraph {/; $ s/$/\n}/' > tmpf.dot && \
  dot -Tpng tmpf.dot -o tmpf.png

python -c 'from pyaux.madness import *; from yadls import db, dbg; dbg.init_logging(); sid = 18; res = [_uprint((sql_f, dbg.pg_exec(sql_f(), [sid])), ret=True) for sql_f in [db.sql_effective_groups, db.sql_effective_members, db.sql_effective_groups_ex, db.sql_effective_members_ex]]'  # noqa

python -c 'from pyaux.madness import *; from yadls import db, dbg; dbg.init_logging(); r = dbg.fill_sample_data(amount=30); print(r)'

python -c 'from pyaux.madness import *; from yadls import db, dbg; dbg.init_logging(); r = _await(dbg.test_engine_aiopg()); print(r)'

== SQLAlchemy session usage ==

Get an object and show related objects:

from __future__ import annotations

from yadls import db, dbg
dbg.init_logging()
dbsess = db.get_session()
Subject = db.Subject
g = (
    dbsess.query(Subject)
    .filter(Subject.name == 'system_group:staff_statleg')
    .one())
print(g, g.__dict__)
print([obj.member.name for obj in g.members])

"""


def init_logging(app_name=None):
    import logging
    try:
        from pyaux.runlib import init_logging
        init_logging(level=1)
    except Exception:
        logging.basicConfig(level=1)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    logging.getLogger('django.db.backends').setLevel(logging.DEBUG)
    logging.getLogger('parso').setLevel(logging.INFO)


async def test_engine_aiopg(engine=None):
    from . import db
    init_logging()
    if engine is None:
        engine = await db.get_engine_aiopg()
    # http://aiopg.readthedocs.io/en/stable/examples.html#simple-sqlalchemy-usage
    async with engine as engine_w:
        async with engine_w.acquire() as conn:
            rows = await conn.execute('''
                select * from (
                    select 'node' as _type, row_to_json(dls_nodes) from dls_nodes
                    union all
                    select 'node_config' as _type, row_to_json(dls_node_config) from dls_node_config
                    union all
                    select 'data' as _type, row_to_json(dls_data) from dls_data
                    union all
                    select 'alembic_version' as _type, row_to_json(dls_alembic_version) from dls_alembic_version
                    union all
                    select 'subject' as _type, row_to_json(dls_subject) from dls_subject
                    union all
                    select 'membership' as _type, row_to_json(dls_group_members_m2m) from dls_group_members_m2m
                ) sq limit 7;''')
            async for row in rows:
                print("multi:", row)
            res = await conn.execute(db.Data.select_().limit(3))
            async for row in res:
                print("data:", row)


async def aiopg_exec(query, *args, **kwargs):
    from aiopg.sa import ResourceClosedError
    from . import db
    engine = await db.get_engine_aiopg()
    async with engine as pool:
        async with pool.acquire() as conn:
            rows = await conn.execute(query, *args, **kwargs)
            results = []
            try:
                async for row in rows:
                    results.append(row)
            except ResourceClosedError:  # e.g. 'This result object does not return rows.'
                # results = []
                pass
    return results


async def dlspg_call(method_name, *args, **kwargs):
    from yadls import db, dbg, manager_aiopg
    dbg.init_logging()
    async with await db.get_engine_aiopg() as db_pool:
        mgr = manager_aiopg.DLSPG(db_cfg=dict(db_pool_writing=db_pool, db_pool_reading=db_pool))
        async with mgr.db.manage(writing=True, tx=True):
            return await getattr(mgr, method_name)(*args, **kwargs)


def pg_exec(query, *args, **kwargs):
    from . import db
    session = db.get_session()
    connection = session.connection()  # pylint: disable=no-member
    result = connection.execute(query, *args, **kwargs)
    result = list(result)
    return result


def fill_sample_data(amount=10, source='randoms'):
    import random
    from . import db
    from .utils import make_uuid

    rnd = random.Random(1)

    session = db.get_session()
    objs = []

    def make_node(scope='system_folder'):
        node = db.Node(uuid=make_uuid(), scope=scope)
        objs.append(node)
        node_cfg = db.NodeConfig(node=node, node_uuid=node.uuid)
        objs.append(node_cfg)
        node.config = node_cfg
        return node

    def make_subject(kind, name, uuid=None):
        cfg = db.NodeConfig(node_uuid=uuid or make_uuid())
        objs.append(cfg)
        subject = db.Subject(kind=kind, name=name, node_config=cfg, source=source)
        objs.append(subject)
        cfg.subject = subject
        return subject

    perm_kinds = ['acl_deny', 'acl_adm', 'acl_view']

    def add_node_permissions(node_config, perm_kind_to_subjects, comment='(autogen)', active=True):
        for perm_kind, subjects in perm_kind_to_subjects.items():
            for subject in subjects:
                perm = db.Grant(
                    active=active, perm_kind=perm_kind,
                    node_config=node_config, subject=subject,
                    meta=dict(comment=comment),
                )
                session.add(perm)  # pylint: disable=no-member

    # SAMPLE_SCOPES_CONFIG = {}
    # conf_item = dict(key='configuration__scopes', data=json.dumps(SAMPLE_SCOPES_CONFIG))
    # session.connection().execute(db_utils.upsert_statement(
    #     table=db.Data, values=conf_item, key=('key',)))

    users = [make_subject(kind='user', name='user:user{}'.format(idx)) for idx in range(amount)]
    groups = [make_subject(kind='group', name='group:group{}'.format(idx)) for idx in range(amount)]
    g_active_user = make_subject(kind='group', name='system_group:all_active_users')
    active_users_cnt = int(len(users) * 0.9)
    objs.extend(
        db.group_members_m2m(group=g_active_user, member=user, source=source)
        for user in users[:active_users_cnt])

    subjects = users + groups
    for group in groups:
        group_subjects = random.sample(subjects, random.randrange(min(amount, 4)))
        for gs in group_subjects:
            objs.append(db.group_members_m2m(group=group, member=gs))

    nodes = [make_node() for _ in range(amount)]

    for node in nodes + groups:
        permissions = {perm_kind: random.sample(subjects, 2) for perm_kind in perm_kinds}
        add_node_permissions(node_config=node.node_config, perm_kind_to_subjects=permissions)

    for obj in objs:
        session.add(obj)  # pylint: disable=no-member

    session.commit()  # pylint: disable=no-member
    return locals()


def group_effective_subjects(id_or_url):
    from . import db, db_walkers
    init_logging()
    session = db.get_session()

    if isinstance(id_or_url, int) or id_or_url.isdigit():
        flt = db.Subject.name == 'group:{}'.format(id_or_url)
    else:
        flt = db.Subject.meta['url_data'].astext == id_or_url

    qs = session.query(  # pylint: disable=no-member
        db.Subject).filter(flt)
    subject = qs.one()
    print("Group:", subject)
    sql = db_walkers.sql_effective_members() % ':group_id'
    id_select = db.sa.text(sql).bindparams(group_id=subject.id)
    where = db.Subject.id.in_(id_select)
    qs = session.query(  # pylint: disable=no-member
        db.Subject).filter(where)
    result = list(qs)
    return sorted(list((item.name, item.meta.get('url_data')) for item in result))


def subject_effective_groups(name):
    from . import db, db_walkers
    init_logging()
    session = db.get_session()

    if ':' not in name:
        name = 'user:{}'.format(name)

    flt = db.Subject.name == name
    qs = session.query(  # pylint: disable=no-member
        db.Subject).filter(flt)
    subject = qs.one()
    print("User:", subject)
    wfunc = db_walkers.sql_effective_groups
    sql = wfunc() % ':member_id'
    id_select = db.sa.text(sql).bindparams(member_id=subject.id)
    where = db.Subject.id.in_(id_select)
    qs = session.query(  # pylint: disable=no-member
        db.Subject).filter(where)
    result = list(qs)
    return sorted(list((item.name, item.meta.get('url_data')) for item in result))


def subject_effective_groups_ex(name, extended=False):
    import sqlalchemy as sa
    import sqlalchemy.dialects.postgresql as sa_pg

    from . import db, db_walkers
    init_logging()
    session = db.get_session()

    if ':' not in name:
        name = 'user:{}'.format(name)

    flt = db.Subject.name == name
    qs = session.query(  # pylint: disable=no-member
        db.Subject).filter(flt)
    subject = qs.one()
    print("User:", subject)
    wfunc = db_walkers.sql_effective_groups_ex
    sql = wfunc() % ':member_id'
    walk_select = sa.text(sql).bindparams(member_id=subject.id)
    walk_select = walk_select.columns(
        group_id=sa.Integer, member_id=sa.Integer,
        depth=sa.Integer, path=sa_pg.ARRAY(sa.Integer))
    walk_select = walk_select.alias('walk_select')
    stmt = db.Subject.__table__.join(walk_select, walk_select.c.group_id == db.Subject.id).select()
    rows = session.execute(stmt)  # pylint: disable=no-member
    rows = list(dict(row) for row in rows)
    return sorted((row['name'], row) for row in rows)


def sql_pretty(sql, reindent=True, keyword_case='upper', style='default', bind=None, dialect=None):
    if not isinstance(sql, str) and hasattr(sql, 'compile'):
        sql = str(sql.compile(bind=bind, dialect=dialect))

    if not isinstance(sql, str):
        sql = str(sql)

    assert sql

    import sqlparse
    import pygments
    import pygments.lexers.sql
    import pygments.formatters.terminal256

    sql = sqlparse.format(sql, reindent=reindent, keyword_case=keyword_case)
    if style is not None:
        sql = pygments.highlight(
            sql,
            pygments.lexers.sql.SqlLexer(),
            pygments.formatters.terminal256.Terminal256Formatter(style=style))
    return sql


def get_or_create_subject(session, data, auto_identifier=False):
    from . import db
    from .utils import make_uuid
    from .db_utils import get_or_create_sasess as get_or_create

    model = db.Subject
    locator = dict(name=data['name'])

    # ... also checks the usage validity before any database requests ...
    data = dict(data)
    if auto_identifier:
        node_identifier = data.pop('node_identifier', None) or str(make_uuid())
    else:
        node_identifier = data.pop('node_identifier')

    instance = session.query(model).filter_by(**locator).first()
    if instance is not None:
        return instance

    nc, _ = get_or_create(
        session=session, model=db.NodeConfig,
        node_identifier=node_identifier)
    subject, _ = get_or_create(
        session=session, model=model,
        node_config=nc, **data)
    return subject


def add_root_superuser():
    from . import db, dbg, utils, manager_aiopg
    from .db_utils import get_or_create_sasess as get_or_create
    dbg.init_logging()
    dbsess = db.get_session()

    root = get_or_create_subject(
        dbsess,
        dict(
            node_identifier=utils.prefixed_uuid('sys_user__root'),
            kind='user',
            name='system_user:root',
            active=True,
            source='system'))

    mgr = manager_aiopg.DLSPGBase()
    group_datas = [mgr.get_active_user_group(), mgr.get_superuser_group()]
    groups = [
        get_or_create_subject(dbsess, dict(gd))
        for gd in group_datas]

    for grp in groups:
        get_or_create(
            dbsess, db.group_members_m2m, group=grp, member=root,
            defaults=dict(source='system'))
    dbsess.commit()  # pylint: disable=no-member
    return root
