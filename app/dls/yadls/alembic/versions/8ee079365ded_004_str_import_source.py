"""004_str_import_source

Revision ID: 8ee079365ded
Revises: 788d56980c13
Create Date: 2018-09-07 14:53:14.109019

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = '8ee079365ded'
down_revision = '788d56980c13'
branch_labels = None
depends_on = None


def upgrade():
    import_source = postgresql.ENUM('unknown', 'other', 'system', 'staff', name='dls_import_source')
    op.alter_column('dls_subject', 'source', existing_type=import_source, type_=sa.String(), server_default="'unknown'", existing_server_default="'unknown'")
    op.alter_column('dls_group_members_m2m', 'source', existing_type=import_source, type_=sa.String, server_default="'unknown'", existing_server_default="'unknown'")
    import_source.drop(op.get_bind())


def downgrade():
    import_source = postgresql.ENUM('unknown', 'other', 'system', 'staff', name='dls_import_source')
    import_source.create(op.get_bind())
    op.execute('alter table dls_subject alter column source type dls_import_source using source::dls_import_source;')
    op.execute('alter table dls_group_members_m2m alter column source type dls_import_source using source::dls_import_source;')
