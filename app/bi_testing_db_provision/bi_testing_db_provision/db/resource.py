from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy import func, ForeignKey
from sqlalchemy.dialects import postgresql

from bi_testing_db_provision.db.base import Base
from bi_testing_db_provision.model.enums import ResourceKind, ResourceState, ResourceType


class ResourceRecord(Base):
    __tablename__ = 'resources'

    id = sa.Column(postgresql.UUID(as_uuid=True), primary_key=True)
    resource_kind = sa.Column(sa.Enum(ResourceKind), nullable=False)
    resource_type = sa.Column(sa.Enum(ResourceType), nullable=False)
    resource_state = sa.Column(sa.Enum(ResourceState), nullable=False)
    create_ts = sa.Column(sa.DateTime(timezone=True), nullable=False, default=func.now())
    update_ts = sa.Column(sa.DateTime(timezone=True), nullable=False)
    resource_internal_data = sa.Column(postgresql.JSONB(), nullable=True)
    resource_external_data = sa.Column(postgresql.JSONB(), nullable=True)
    resource_request = sa.Column(postgresql.JSONB(), nullable=False)
    resource_request_hash = sa.Column(postgresql.UUID(as_uuid=True), nullable=False)
    session_id = sa.Column(postgresql.UUID(as_uuid=True), ForeignKey('sessions.id'), nullable=True, index=True)
