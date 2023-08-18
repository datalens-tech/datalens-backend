"""005_ensure_system_subjects

Revision ID: b31f393049a7
Revises: 8ee079365ded
Create Date: 2018-10-10 18:46:57.350306

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import table
import sqlalchemy.dialects.postgresql as pg


# revision identifiers, used by Alembic.
revision = 'b31f393049a7'
down_revision = '8ee079365ded'
branch_labels = None
depends_on = None


# pylint: disable=redefined-outer-name
def upsert_statement(table, values, key, **kwargs):
    stmt = pg.insert(table).values(**values)
    stmt = stmt.on_conflict_do_update(
        index_elements=key,
        set_=values,
        **kwargs)
    return stmt


def upgrade():
    subject_kind = pg.ENUM(
        'user', 'group', 'other',
        name='dls_subject_kind')
    NodeConfig = table(
        'dls_node_config',

        sa.Column('id', sa.Integer, primary_key=True, autoincrement=True, nullable=False),
        sa.Column('meta', pg.JSONB, server_default="'{}'", nullable=False),
        sa.Column('ctime', pg.TIMESTAMP(precision=6, timezone=True), server_default=sa.func.now(), index=True),
        sa.Column('mtime', pg.TIMESTAMP(precision=6, timezone=True), server_default=sa.func.now(), index=True, onupdate=sa.func.current_timestamp()),

        sa.Column('node_identifier', sa.String, index=True, nullable=False, unique=True),
        sa.Column(
            'node_id',
            sa.Integer, sa.ForeignKey('dls_nodes.id', name='fk_dls_nodes_id'),
            index=True, nullable=True),
        # sa_orm.relationship(
        #     'node',
        #     Node,
        #     backref=sa_orm.backref("node_config", uselist=False)),
    )
    Subject = table(
        'dls_subject',

        sa.Column('id', sa.Integer, primary_key=True, autoincrement=True, nullable=False),
        sa.Column('meta', pg.JSONB, server_default="'{}'", nullable=False),
        sa.Column('ctime', pg.TIMESTAMP(precision=6, timezone=True), server_default=sa.func.now(), index=True),
        sa.Column('mtime', pg.TIMESTAMP(precision=6, timezone=True), server_default=sa.func.now(), index=True, onupdate=sa.func.current_timestamp()),

        sa.Column('kind', subject_kind, index=True, nullable=False),
        sa.Column('name', sa.String, index=True, nullable=False, unique=True),
        sa.Column(
            'node_config_id',
            sa.Integer, sa.ForeignKey('dls_node_config.id', name='fk_node_config_id'),
            index=True, nullable=False),
        # sa_orm.relationship('node_config', NodeConfig, backref=sa_orm.backref("subject", uselist=False)),
        sa.Column('active', sa.Boolean, server_default="true", index=True, nullable=False),
        sa.Column('source', sa.String, index=True, nullable=False, server_default="'unknown'"),
    )
    group_members_m2m = table(
        'dls_group_members_m2m',
        sa.Column(
            'group_id',
            sa.Integer, sa.ForeignKey('dls_subject.id', name='fk_group_subject_id'),
            primary_key=True),
        sa.Column(
            'member_id',
            sa.Integer, sa.ForeignKey('dls_subject.id', name='fk_member_subject_id'),
            primary_key=True),

        # sa_orm.relationship(
        #     'group',
        #     Subject, primaryjoin=group_id == Subject.id,
        #     backref='members'),
        # sa_orm.relationship(
        #     'member',
        #     Subject, primaryjoin=member_id == Subject.id,
        #     backref='memberships_direct'),
        sa.Column('source', sa.String, index=True, nullable=False, server_default="'unknown'"),

        sa.Column('meta', pg.JSONB, server_default="'{}'", nullable=False),
        sa.Column('ctime', pg.TIMESTAMP(precision=6, timezone=True), server_default=sa.func.now(), index=True),
        sa.Column('mtime', pg.TIMESTAMP(precision=6, timezone=True), server_default=sa.func.now(), index=True, onupdate=sa.func.current_timestamp()),
    )

    commons = dict(
        source='system',
        active=True,
    )
    subjects = (
        dict(
            commons,
            name='system_group:all_active_users',
            kind='group',
            meta=dict(
                title=dict(
                    ru='Все',
                    en='Everyone',
                ),
            ),
            node_identifier='735f675f-6163-7469-7665-000000000000'),
        dict(
            commons,
            name='system_group:superuser',
            kind='group',
            meta=dict(),
            node_identifier='735f675f-7375-7065-7275-736572000000'),
        dict(
            commons,
            name='system_user:root',
            kind='user',
            meta=dict(),
            node_identifier='7379735f-7573-6572-5f5f-726f6f740000'),
    )
    memberships = (
        dict(group='system_group:all_active_users', member='system_user:root'),
        dict(group='system_group:superuser', member='system_user:root'),
    )
    subj_ids = {}

    conn = op.get_bind()  # pylint: disable=no-member

    def _get_single_value(exec_res):
        return next(exec_res)[0]

    for subj in subjects:
        stmt = upsert_statement(
            table=NodeConfig,
            values=dict(node_identifier=subj['node_identifier']),
            key=('node_identifier',),
        ).returning(NodeConfig.c.id)
        res = conn.execute(stmt)
        ncid = _get_single_value(res)
        subj_data = dict(subj, node_config_id=ncid)
        subj_data.pop('node_identifier', None)
        stmt = upsert_statement(
            table=Subject,
            values=subj_data,
            key=('name',),
        ).returning(Subject.c.id)
        res = conn.execute(stmt)
        subj_ids[subj['name']] = _get_single_value(res)

    for membership in memberships:
        membership_data = dict(
            source=membership.get('source') or 'system',
            meta=membership.get('meta') or {},
            group_id=subj_ids[membership['group']],
            member_id=subj_ids[membership['member']],
        )
        stmt = upsert_statement(
            table=group_members_m2m,
            values=membership_data,
            key=('group_id', 'member_id'))
        conn.execute(stmt)


def downgrade():
    pass  # Nothing to do here, pretty much.
