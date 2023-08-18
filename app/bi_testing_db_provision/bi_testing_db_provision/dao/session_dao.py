from datetime import timezone, datetime
from typing import Dict, Any, Optional

import attr
from aiopg.sa.result import RowProxy

from bi_testing_db_provision.dao.base_dao import BaseDAO
from bi_testing_db_provision.db.session import SessionRecord
from bi_testing_db_provision.db_connection import DBConn
from bi_testing_db_provision.model.session import Session


@attr.s
class SessionDAO(BaseDAO[Session]):
    record_cls = SessionRecord
    update_notification_channel = 'session_update'

    _conn: DBConn = attr.ib()

    def _model_to_record(self, session: Session, for_create: bool = False) -> Dict[str, Any]:
        values = dict(
            state=session.state,
            update_ts=session.create_ts,
            description=session.description,
        )
        if for_create:
            values.update(
                id=session.id,
                create_ts=session.create_ts,
            )
        return values

    def _record_to_model(self, record: Optional[RowProxy]) -> Optional[Session]:  # type: ignore  # TODO: fix
        if record is None:
            return None

        return Session(
            id=record['id'],
            state=record['state'],
            create_ts=record['create_ts'],
            update_ts=record['update_ts'],
            description=record['description'],
        )

    def _prepare_model_for_create(self, model: Session) -> Session:
        model = super()._prepare_model_for_create(model)
        now = datetime.now(timezone.utc)
        return model.clone(create_ts=now, update_ts=now)

    def _prepare_model_for_update(self, model: Session) -> Session:
        now = datetime.now(timezone.utc)
        return model.clone(update_ts=now)

    def _model_to_notification_event(self, model: Session) -> Dict[str, Any]:
        return dict(
            id=model.id.hex,  # type: ignore  # TODO: fix
            state=model.state.name,
        )
