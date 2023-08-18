"""003_uuid_to_identifier

Revision ID: 788d56980c13
Revises: f060464a7028
Create Date: 2018-08-27 18:37:03.200167

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import table

# revision identifiers, used by Alembic.
revision = '788d56980c13'
down_revision = 'f060464a7028'
branch_labels = None
depends_on = None


def upgrade():
    # + Node.identifier
    op.add_column('dls_nodes', sa.Column('identifier', sa.String(), nullable=True))
    # + NodeConfig.node_identifier
    op.add_column('dls_node_config', sa.Column('node_identifier', sa.String(), nullable=True))
    # + Log.node_identifier
    op.add_column('dls_log', sa.Column('node_identifier', sa.String(), nullable=True))

    Node = table(
        'dls_nodes',
        sa.Column('uuid', postgresql.UUID, index=True, nullable=False, unique=True),
        sa.Column('identifier', sa.String, index=True, nullable=False, unique=True),
        # ...
    )
    NodeConfig = table(
        'dls_node_config',
        sa.Column('node_uuid', postgresql.UUID, index=True, nullable=False, unique=True),
        sa.Column('node_identifier', sa.String, index=True, nullable=True, unique=True),
    )
    Log = table(
        'dls_log',
        sa.Column(
            'node_uuid',
            postgresql.UUID, sa.ForeignKey(NodeConfig.c.node_uuid, name='fk_node_uuid'),
            index=True, nullable=True),
        sa.Column(
            'node_identifier',
            sa.String, sa.ForeignKey(NodeConfig.c.node_identifier, name='fk_node_identifier'),
            index=True, nullable=True),
    )

    op.execute(
        Node
        .update()
        .where(Node.c.identifier == None)
        .values(dict(identifier=Node.c.uuid))
    )
    op.execute(
        NodeConfig
        .update()
        .where(NodeConfig.c.node_identifier == None)
        .values(dict(node_identifier=NodeConfig.c.node_uuid))
    )
    op.execute(
        Log
        .update()
        .where(Log.c.node_identifier == None)
        .values(dict(node_identifier=Log.c.node_uuid))
    )

    # - Log.node_uuid
    op.drop_constraint('fk_node_uuid', 'dls_log', type_='foreignkey')
    op.drop_index('ix_dls_log_node_uuid', table_name='dls_log')
    op.drop_column('dls_log', 'node_uuid')
    # - NodeConfig.node_uuid
    op.drop_index('ix_dls_node_config_node_uuid', table_name='dls_node_config')
    op.drop_column('dls_node_config', 'node_uuid')
    # - Node.uuid
    op.drop_index('ix_dls_nodes_uuid', table_name='dls_nodes')
    op.drop_column('dls_nodes', 'uuid')

    # + indexes
    op.create_index(op.f('ix_dls_nodes_identifier'), 'dls_nodes', ['identifier'], unique=True)
    op.create_index(op.f('ix_dls_node_config_node_identifier'), 'dls_node_config', ['node_identifier'], unique=True)
    op.create_index(op.f('ix_dls_log_node_identifier'), 'dls_log', ['node_identifier'], unique=False)
    # + FK
    op.create_foreign_key('fk_node_identifier', 'dls_log', 'dls_node_config', ['node_identifier'], ['node_identifier'])
    # - nullable
    op.alter_column('dls_nodes', 'identifier', nullable=False)
    op.alter_column('dls_node_config', 'node_identifier', nullable=False)


def downgrade():
    # + Node.uuid
    op.add_column('dls_nodes', sa.Column('uuid', postgresql.UUID(), autoincrement=False, nullable=True))
    # + NodeConfig.node_uuid
    op.add_column('dls_node_config', sa.Column('node_uuid', postgresql.UUID(), autoincrement=False, nullable=True))
    # + Log.node_uuid
    op.add_column('dls_log', sa.Column('node_uuid', postgresql.UUID(), autoincrement=False, nullable=True))

    Node = table(
        'dls_nodes',
        sa.Column('uuid', postgresql.UUID, index=True, nullable=False, unique=True),
        sa.Column('identifier', sa.String, index=True, nullable=False, unique=True),
        # ...
    )
    NodeConfig = table(
        'dls_node_config',
        sa.Column('node_uuid', postgresql.UUID, index=True, nullable=False, unique=True),
        sa.Column('node_identifier', sa.String, index=True, nullable=True, unique=True),
    )
    Log = table(
        'dls_log',
        sa.Column(
            'node_uuid',
            postgresql.UUID, sa.ForeignKey(NodeConfig.c.node_uuid, name='fk_node_uuid'),
            index=True, nullable=True),
        sa.Column(
            'node_identifier',
            sa.String, sa.ForeignKey(NodeConfig.node_identifier, name='fk_node_identifier'),
            index=True, nullable=True),
    )

    op.execute(
        Node
        .update()
        .where(Node.c.uuid == None)
        .values(dict(uuid=Node.c.identifier))
    )
    op.execute(
        NodeConfig
        .update()
        .where(NodeConfig.c.node_uuid == None)
        .values(dict(node_uuid=NodeConfig.c.node_identifier))
    )
    op.execute(
        Log
        .update()
        .where(Log.c.node_uuid == None)
        .values(dict(node_uuid=Log.c.node_identifier))
    )

    # - Log.node_identifier
    op.drop_constraint('fk_node_identifier', 'dls_log', type_='foreignkey')
    op.drop_index(op.f('ix_dls_log_node_identifier'), table_name='dls_log')
    op.drop_column('dls_log', 'node_identifier')
    # - NodeConfig.node_identifier
    op.drop_index(op.f('ix_dls_node_config_node_identifier'), table_name='dls_node_config')
    op.drop_column('dls_node_config', 'node_identifier')
    # - Node.identifier
    op.drop_index(op.f('ix_dls_nodes_identifier'), table_name='dls_nodes')
    op.drop_column('dls_nodes', 'identifier')

    # + indexes
    op.create_index('ix_dls_nodes_uuid', 'dls_nodes', ['uuid'], unique=True)
    op.create_index('ix_dls_node_config_node_uuid', 'dls_node_config', ['node_uuid'], unique=True)
    op.create_index('ix_dls_log_node_uuid', 'dls_log', ['node_uuid'], unique=False)
    # + FK
    op.create_foreign_key('fk_node_uuid', 'dls_log', 'dls_node_config', ['node_uuid'], ['node_uuid'])
    # - nullable
    op.alter_column('dls_nodes', 'uuid', nullable=False)
    op.alter_column('dls_node_config', 'node_uuid', nullable=False)
