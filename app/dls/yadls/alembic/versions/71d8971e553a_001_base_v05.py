"""001_base_v05

Revision ID: 71d8971e553a
Revises:
Create Date: 2018-07-17 14:51:59.038562

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '71d8971e553a'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('dls_data',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('meta', postgresql.JSONB(astext_type=sa.Text()), server_default='{}', nullable=False),
    sa.Column('ctime', postgresql.TIMESTAMP(timezone=True, precision=6), server_default=sa.text('now()'), nullable=True),
    sa.Column('mtime', postgresql.TIMESTAMP(timezone=True, precision=6), server_default=sa.text('now()'), nullable=True),
    sa.Column('key', sa.String(), nullable=False),
    sa.Column('data', sa.String(), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_dls_data_ctime'), 'dls_data', ['ctime'], unique=False)
    op.create_index(op.f('ix_dls_data_key'), 'dls_data', ['key'], unique=True)
    op.create_index(op.f('ix_dls_data_mtime'), 'dls_data', ['mtime'], unique=False)
    op.create_table('dls_nodes',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('uuid', postgresql.UUID(), nullable=False),
    sa.Column('scope', sa.String(), nullable=False),
    sa.Column('meta', postgresql.JSONB(astext_type=sa.Text()), server_default='{}', nullable=False),
    sa.Column('ctime', postgresql.TIMESTAMP(timezone=True, precision=6), server_default=sa.text('now()'), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_dls_nodes_ctime'), 'dls_nodes', ['ctime'], unique=False)
    op.create_index(op.f('ix_dls_nodes_scope'), 'dls_nodes', ['scope'], unique=False)
    op.create_index(op.f('ix_dls_nodes_uuid'), 'dls_nodes', ['uuid'], unique=True)
    op.create_table('dls_periodic_task',
    sa.Column('name', sa.String(), nullable=False),
    sa.Column('frequency', sa.Float(), nullable=True),
    sa.Column('lock_expire', sa.Float(), nullable=True),
    sa.Column('lock_renew', sa.Float(), nullable=True),
    sa.Column('lock', sa.String(), nullable=True),
    sa.Column('last_start_ts', postgresql.TIMESTAMP(timezone=True, precision=6), nullable=True),
    sa.Column('last_success_ts', postgresql.TIMESTAMP(timezone=True, precision=6), nullable=True),
    sa.Column('last_failure_ts', postgresql.TIMESTAMP(timezone=True, precision=6), nullable=True),
    sa.Column('last_ping_ts', postgresql.TIMESTAMP(timezone=True, precision=6), nullable=True),
    sa.Column('last_ping_meta', postgresql.JSONB(astext_type=sa.Text()), server_default='{}', nullable=False),
    sa.Column('meta', postgresql.JSONB(astext_type=sa.Text()), server_default='{}', nullable=False),
    sa.PrimaryKeyConstraint('name')
    )
    op.create_table('dls_ss_word',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('word', sa.String(), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_dls_ss_word_word'), 'dls_ss_word', ['word'], unique=True)
    op.create_table('dls_node_config',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('meta', postgresql.JSONB(astext_type=sa.Text()), server_default='{}', nullable=False),
    sa.Column('ctime', postgresql.TIMESTAMP(timezone=True, precision=6), server_default=sa.text('now()'), nullable=True),
    sa.Column('mtime', postgresql.TIMESTAMP(timezone=True, precision=6), server_default=sa.text('now()'), nullable=True),
    sa.Column('node_uuid', postgresql.UUID(), nullable=False),
    sa.Column('node_id', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['node_id'], ['dls_nodes.id'], name='fk_dls_nodes_id'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_dls_node_config_ctime'), 'dls_node_config', ['ctime'], unique=False)
    op.create_index(op.f('ix_dls_node_config_mtime'), 'dls_node_config', ['mtime'], unique=False)
    op.create_index(op.f('ix_dls_node_config_node_id'), 'dls_node_config', ['node_id'], unique=False)
    op.create_index(op.f('ix_dls_node_config_node_uuid'), 'dls_node_config', ['node_uuid'], unique=True)

    dls_subject_kind = postgresql.ENUM('user', 'group', 'other', name='dls_subject_kind')
    # dls_subject_kind.create(op.get_bind())
    dls_import_source = postgresql.ENUM('unknown', 'other', 'system', 'staff', name='dls_import_source')
    # dls_import_source.create(op.get_bind())

    op.create_table('dls_subject',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('meta', postgresql.JSONB(astext_type=sa.Text()), server_default='{}', nullable=False),
    sa.Column('ctime', postgresql.TIMESTAMP(timezone=True, precision=6), server_default=sa.text('now()'), nullable=True),
    sa.Column('mtime', postgresql.TIMESTAMP(timezone=True, precision=6), server_default=sa.text('now()'), nullable=True),
    sa.Column('kind', dls_subject_kind, nullable=False),
    sa.Column('name', sa.String(), nullable=False),
    sa.Column('node_config_id', sa.Integer(), nullable=False),
    sa.Column('active', sa.Boolean(), server_default='true', nullable=False),
    sa.Column('source', dls_import_source, server_default='unknown', nullable=False),
    sa.ForeignKeyConstraint(['node_config_id'], ['dls_node_config.id'], name='fk_node_config_id'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_dls_subject_active'), 'dls_subject', ['active'], unique=False)
    op.create_index(op.f('ix_dls_subject_ctime'), 'dls_subject', ['ctime'], unique=False)
    op.create_index(op.f('ix_dls_subject_kind'), 'dls_subject', ['kind'], unique=False)
    op.create_index(op.f('ix_dls_subject_mtime'), 'dls_subject', ['mtime'], unique=False)
    op.create_index(op.f('ix_dls_subject_name'), 'dls_subject', ['name'], unique=True)
    op.create_index(op.f('ix_dls_subject_node_config_id'), 'dls_subject', ['node_config_id'], unique=False)
    op.create_index(op.f('ix_dls_subject_source'), 'dls_subject', ['source'], unique=False)
    op.create_table('dls_grant',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('guid', postgresql.UUID(), nullable=True),
    sa.Column('perm_kind', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('node_config_id', sa.Integer(), nullable=True),
    sa.Column('subject_id', sa.Integer(), nullable=True),
    sa.Column('active', sa.Boolean(), nullable=False),
    sa.Column('state', sa.String(), server_default='default', nullable=False),
    sa.Column('meta', postgresql.JSONB(astext_type=sa.Text()), server_default='{}', nullable=False),
    sa.Column('ctime', postgresql.TIMESTAMP(timezone=True, precision=6), server_default=sa.text('now()'), nullable=True),
    sa.Column('mtime', postgresql.TIMESTAMP(timezone=True, precision=6), server_default=sa.text('now()'), nullable=True),
    sa.ForeignKeyConstraint(['node_config_id'], ['dls_node_config.id'], name='fk_node_config_id'),
    sa.ForeignKeyConstraint(['subject_id'], ['dls_subject.id'], name='fk_subject_id'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('node_config_id', 'subject_id', name='_grant_locator')
    )
    op.create_index(op.f('ix_dls_grant_active'), 'dls_grant', ['active'], unique=False)
    op.create_index(op.f('ix_dls_grant_ctime'), 'dls_grant', ['ctime'], unique=False)
    op.create_index(op.f('ix_dls_grant_guid'), 'dls_grant', ['guid'], unique=True)
    op.create_index(op.f('ix_dls_grant_mtime'), 'dls_grant', ['mtime'], unique=False)
    op.create_index(op.f('ix_dls_grant_node_config_id'), 'dls_grant', ['node_config_id'], unique=False)
    op.create_index(op.f('ix_dls_grant_state'), 'dls_grant', ['state'], unique=False)
    op.create_index(op.f('ix_dls_grant_subject_id'), 'dls_grant', ['subject_id'], unique=False)
    op.create_table('dls_group_members_m2m',
    sa.Column('group_id', sa.Integer(), nullable=False),
    sa.Column('member_id', sa.Integer(), nullable=False),
    sa.Column('source', dls_import_source, server_default='unknown', nullable=False),
    sa.Column('meta', postgresql.JSONB(astext_type=sa.Text()), server_default='{}', nullable=False),
    sa.Column('ctime', postgresql.TIMESTAMP(timezone=True, precision=6), server_default=sa.text('now()'), nullable=True),
    sa.Column('mtime', postgresql.TIMESTAMP(timezone=True, precision=6), server_default=sa.text('now()'), nullable=True),
    sa.ForeignKeyConstraint(['group_id'], ['dls_subject.id'], name='fk_group_subject_id'),
    sa.ForeignKeyConstraint(['member_id'], ['dls_subject.id'], name='fk_member_subject_id'),
    sa.PrimaryKeyConstraint('group_id', 'member_id')
    )
    op.create_index(op.f('ix_dls_group_members_m2m_ctime'), 'dls_group_members_m2m', ['ctime'], unique=False)
    op.create_index(op.f('ix_dls_group_members_m2m_mtime'), 'dls_group_members_m2m', ['mtime'], unique=False)
    op.create_index(op.f('ix_dls_group_members_m2m_source'), 'dls_group_members_m2m', ['source'], unique=False)
    op.create_table('dls_ss_word_subjects',
    sa.Column('ssword_id', sa.Integer(), nullable=False),
    sa.Column('subject_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['ssword_id'], ['dls_ss_word.id'], name='fk_ssword_id'),
    sa.ForeignKeyConstraint(['subject_id'], ['dls_subject.id'], name='fk_subject_id'),
    sa.PrimaryKeyConstraint('ssword_id', 'subject_id')
    )
    op.create_index(op.f('ix_dls_ss_word_subjects_ssword_id'), 'dls_ss_word_subjects', ['ssword_id'], unique=False)
    op.create_index(op.f('ix_dls_ss_word_subjects_subject_id'), 'dls_ss_word_subjects', ['subject_id'], unique=False)
    op.create_table('dls_subject_memberships_m2m',
    sa.Column('subject_id', sa.Integer(), nullable=False),
    sa.Column('group_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['group_id'], ['dls_subject.id'], name='fk_group_subject_id'),
    sa.ForeignKeyConstraint(['subject_id'], ['dls_subject.id'], name='fk_subject_id'),
    sa.PrimaryKeyConstraint('subject_id', 'group_id')
    )
    op.create_table('dls_log',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('meta', postgresql.JSONB(astext_type=sa.Text()), server_default='{}', nullable=False),
    sa.Column('ctime', postgresql.TIMESTAMP(timezone=True, precision=6), server_default=sa.text('now()'), nullable=True),
    sa.Column('mtime', postgresql.TIMESTAMP(timezone=True, precision=6), server_default=sa.text('now()'), nullable=True),
    sa.Column('kind', sa.String(), server_default='etc', nullable=False),
    sa.Column('sublocator', sa.String(), server_default='', nullable=False),
    sa.Column('grant_guid', postgresql.UUID(), nullable=True),
    sa.Column('request_user_id', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['grant_guid'], ['dls_grant.guid'], name='fk_grant_guid'),
    sa.ForeignKeyConstraint(['request_user_id'], ['dls_subject.id'], name='fk_request_user_subject_id'),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_dls_log_ctime'), 'dls_log', ['ctime'], unique=False)
    op.create_index(op.f('ix_dls_log_grant_guid'), 'dls_log', ['grant_guid'], unique=False)
    op.create_index(op.f('ix_dls_log_kind'), 'dls_log', ['kind'], unique=False)
    op.create_index(op.f('ix_dls_log_mtime'), 'dls_log', ['mtime'], unique=False)
    op.create_index(op.f('ix_dls_log_request_user_id'), 'dls_log', ['request_user_id'], unique=False)
    op.create_index(op.f('ix_dls_log_sublocator'), 'dls_log', ['sublocator'], unique=False)
    # ### end Alembic commands ###


def downgrade():
    op.drop_index(op.f('ix_dls_log_sublocator'), table_name='dls_log')
    op.drop_index(op.f('ix_dls_log_request_user_id'), table_name='dls_log')
    op.drop_index(op.f('ix_dls_log_mtime'), table_name='dls_log')
    op.drop_index(op.f('ix_dls_log_kind'), table_name='dls_log')
    op.drop_index(op.f('ix_dls_log_grant_guid'), table_name='dls_log')
    op.drop_index(op.f('ix_dls_log_ctime'), table_name='dls_log')
    op.drop_table('dls_log')
    op.drop_table('dls_subject_memberships_m2m')
    op.drop_index(op.f('ix_dls_ss_word_subjects_subject_id'), table_name='dls_ss_word_subjects')
    op.drop_index(op.f('ix_dls_ss_word_subjects_ssword_id'), table_name='dls_ss_word_subjects')
    op.drop_table('dls_ss_word_subjects')
    op.drop_index(op.f('ix_dls_group_members_m2m_source'), table_name='dls_group_members_m2m')
    op.drop_index(op.f('ix_dls_group_members_m2m_mtime'), table_name='dls_group_members_m2m')
    op.drop_index(op.f('ix_dls_group_members_m2m_ctime'), table_name='dls_group_members_m2m')
    op.drop_table('dls_group_members_m2m')
    op.drop_index(op.f('ix_dls_grant_subject_id'), table_name='dls_grant')
    op.drop_index(op.f('ix_dls_grant_state'), table_name='dls_grant')
    op.drop_index(op.f('ix_dls_grant_node_config_id'), table_name='dls_grant')
    op.drop_index(op.f('ix_dls_grant_mtime'), table_name='dls_grant')
    op.drop_index(op.f('ix_dls_grant_guid'), table_name='dls_grant')
    op.drop_index(op.f('ix_dls_grant_ctime'), table_name='dls_grant')
    op.drop_index(op.f('ix_dls_grant_active'), table_name='dls_grant')
    op.drop_table('dls_grant')
    op.drop_index(op.f('ix_dls_subject_source'), table_name='dls_subject')
    op.drop_index(op.f('ix_dls_subject_node_config_id'), table_name='dls_subject')
    op.drop_index(op.f('ix_dls_subject_name'), table_name='dls_subject')
    op.drop_index(op.f('ix_dls_subject_mtime'), table_name='dls_subject')
    op.drop_index(op.f('ix_dls_subject_kind'), table_name='dls_subject')
    op.drop_index(op.f('ix_dls_subject_ctime'), table_name='dls_subject')
    op.drop_index(op.f('ix_dls_subject_active'), table_name='dls_subject')
    op.drop_table('dls_subject')
    op.drop_index(op.f('ix_dls_node_config_node_uuid'), table_name='dls_node_config')
    op.drop_index(op.f('ix_dls_node_config_node_id'), table_name='dls_node_config')
    op.drop_index(op.f('ix_dls_node_config_mtime'), table_name='dls_node_config')
    op.drop_index(op.f('ix_dls_node_config_ctime'), table_name='dls_node_config')
    op.drop_table('dls_node_config')
    op.drop_index(op.f('ix_dls_ss_word_word'), table_name='dls_ss_word')
    op.drop_table('dls_ss_word')
    op.drop_table('dls_periodic_task')
    op.drop_index(op.f('ix_dls_nodes_uuid'), table_name='dls_nodes')
    op.drop_index(op.f('ix_dls_nodes_scope'), table_name='dls_nodes')
    op.drop_index(op.f('ix_dls_nodes_ctime'), table_name='dls_nodes')
    op.drop_table('dls_nodes')
    op.drop_index(op.f('ix_dls_data_mtime'), table_name='dls_data')
    op.drop_index(op.f('ix_dls_data_key'), table_name='dls_data')
    op.drop_index(op.f('ix_dls_data_ctime'), table_name='dls_data')
    op.drop_table('dls_data')

    dls_import_source = postgresql.ENUM('unknown', 'other', 'system', 'staff', name='dls_import_source')
    dls_import_source.drop(op.get_bind())
    dls_subject_kind = postgresql.ENUM('user', 'group', 'other', name='dls_subject_kind')
    dls_subject_kind.drop(op.get_bind())
