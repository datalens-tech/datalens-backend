"""Adding new alert type

Revision ID: 77360c134c23
Revises: 1074e6ab9bbf
Create Date: 2021-06-09 15:17:10.786560

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '77360c134c23'
down_revision = '1074e6ab9bbf'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("COMMIT")
    op.execute("ALTER TYPE alerttype ADD VALUE 'absrelative'")


def downgrade():
    pass
