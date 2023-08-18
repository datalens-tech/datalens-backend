"""initial

Revision ID: cd42de7f203d
Revises: 
Create Date: 2021-04-13 13:23:58.554389

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'cd42de7f203d'
down_revision = None
branch_labels = None
depends_on = None

SessionStateType = sa.Enum(
    'active',
    'deleted',
    name='sessionstate'
)
ResourceKindType = sa.Enum('single_docker', name='resourcekind')
ResourceTypeType = sa.Enum(
    'standalone_postgres',
    'standalone_clickhouse',
    'standalone_mssql',
    'standalone_oracle',
    name='resourcetype'
)
ResourceStateType = sa.Enum(
    'free',
    'acquired',
    'create_required',
    'create_in_progress',
    'exhausted',
    'recycle_required',
    'recycle_in_progress',
    'deleted',
    name='resourcestate'
)


def upgrade():
    op.create_table(
        'sessions',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('state', SessionStateType, nullable=False),
        sa.Column('create_ts', sa.DateTime(timezone=True), nullable=False),
        sa.Column('update_ts', sa.DateTime(timezone=True), nullable=False),
        sa.Column('description', sa.String(length=255), nullable=True),
        sa.PrimaryKeyConstraint('id', name=op.f('pk__sessions'))
    )
    op.create_table(
        'resources',
        sa.Column('id', postgresql.UUID(), nullable=False),
        sa.Column('resource_kind', ResourceKindType, nullable=False),
        sa.Column('resource_type', ResourceTypeType, nullable=False),
        sa.Column('resource_state', ResourceStateType, nullable=False),
        sa.Column('create_ts', sa.DateTime(timezone=True), nullable=False),
        sa.Column('update_ts', sa.DateTime(timezone=True), nullable=False),
        sa.Column('resource_internal_data', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('resource_external_data', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('resource_request', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('resource_request_hash', postgresql.UUID(), nullable=False),
        sa.Column('session_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.PrimaryKeyConstraint('id', name=op.f('pk__resources'))
    )
    op.create_index(op.f('ix__resources__session_id'), 'resources', ['session_id'], unique=False)
    op.create_foreign_key(op.f('fk__resources__session_id__sessions'), 'resources', 'sessions', ['session_id'], ['id'])


def downgrade():
    op.drop_table('resources')
    op.drop_table('sessions')

    SessionStateType.drop(bind=op.get_bind())
    ResourceKindType.drop(bind=op.get_bind())
    ResourceStateType.drop(bind=op.get_bind())
    ResourceTypeType.drop(bind=op.get_bind())
