"""007_nodeconfig_scope

Revision ID: 761bc84f254a
Revises: f319293eb5a3
Create Date: 2018-10-19 16:27:58.684439

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import table
import sqlalchemy.dialects.postgresql as pg


# revision identifiers, used by Alembic.
revision = '761bc84f254a'
down_revision = 'f319293eb5a3'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('dls_node_config', sa.Column('scope', sa.String(), server_default='', nullable=False))

    # Node = table(
    #     'dls_nodes',
    #     sa.Column('id', sa.Integer, primary_key=True, autoincrement=True, nullable=False),
    #     sa.Column('meta', pg.JSONB, server_default="'{}'", nullable=False),
    #     sa.Column('ctime', pg.TIMESTAMP(precision=6, timezone=True), server_default=sa.func.now(), index=True),
    #     sa.Column('mtime', pg.TIMESTAMP(precision=6, timezone=True), server_default=sa.func.now(), index=True, onupdate=sa.func.current_timestamp()),
    #     sa.Column('realm', sa.String, server_default="''", nullable=False, index=True),
    #     sa.Column('identifier', sa.String, index=True, nullable=False, unique=True),
    #     sa.Column('scope', sa.String, index=True, nullable=False),
    # )
    # NodeConfig = table(
    #     'dls_node_config',
    #     sa.Column('id', sa.Integer, primary_key=True, autoincrement=True, nullable=False),
    #     sa.Column('meta', pg.JSONB, server_default="'{}'", nullable=False),
    #     sa.Column('ctime', pg.TIMESTAMP(precision=6, timezone=True), server_default=sa.func.now(), index=True),
    #     sa.Column('mtime', pg.TIMESTAMP(precision=6, timezone=True), server_default=sa.func.now(), index=True, onupdate=sa.func.current_timestamp()),
    #     sa.Column('realm', sa.String, server_default="''", nullable=False, index=True),
    #     sa.Column('node_identifier', sa.String, index=True, nullable=False, unique=True),
    #     sa.Column(
    #         'node_id',
    #         sa.Integer, sa.ForeignKey('dls_nodes.id', name='fk_dls_nodes_id'),
    #         index=True, nullable=True),
    #     sa.Column('scope', sa.String, index=True, nullable=False),
    # )

    op.execute(
        '''update dls_node_config nc set scope = n.scope from dls_nodes n where n.id = nc.node_id;'''
    )
    op.execute(
        '''update dls_node_config nc set scope = s.kind from dls_subject s where s.node_config_id = nc.id;'''
    )

    op.create_index(op.f('ix_dls_node_config_scope'), 'dls_node_config', ['scope'], unique=False)


def downgrade():
    op.drop_index(op.f('ix_dls_node_config_scope'), table_name='dls_node_config')
    op.drop_column('dls_node_config', 'scope')
