from __future__ import annotations

from datetime import datetime
from typing import Optional, Any

import attr

from bi_testing_db_provision.model.base import BaseStoredModel
from bi_testing_db_provision.model.enums import SessionState


@attr.s(frozen=True)
class Session(BaseStoredModel):
    state: SessionState = attr.ib()
    create_ts: datetime = attr.ib(default=None)
    update_ts: datetime = attr.ib(default=None)
    description: Optional[str] = attr.ib(default=None)

    def clone(self, **updated: Any) -> Session:
        return attr.evolve(self, **updated)
