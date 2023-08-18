from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy import func
from sqlalchemy.dialects import postgresql

from bi_testing_db_provision.db.base import Base
from bi_testing_db_provision.model.enums import SessionState


class SessionRecord(Base):
    __tablename__ = 'sessions'

    id = sa.Column(postgresql.UUID(as_uuid=True), primary_key=True)
    state = sa.Column(sa.Enum(SessionState), nullable=False)
    create_ts = sa.Column(sa.DateTime(timezone=True), nullable=False, default=func.now())
    update_ts = sa.Column(sa.DateTime(timezone=True), nullable=False)
    description = sa.Column(sa.String(length=255), nullable=True)
