import abc
import asyncio
import logging
import uuid
from typing import TypeVar, Generic, Optional, ClassVar, Type, Dict, Any

import attr
from aiopg.sa.result import RowProxy

from bi_testing_db_provision.db.base import Base
from bi_testing_db_provision.db_connection import DBConn
from bi_testing_db_provision.model.base import BaseStoredModel

LOGGER = logging.getLogger(__name__)

_STORED_TV = TypeVar("_STORED_TV", bound=BaseStoredModel)


@attr.s
class BaseDAO(Generic[_STORED_TV]):
    record_cls: ClassVar[Type[Base]]
    update_notification_channel: ClassVar[str]

    _conn: DBConn = attr.ib()

    @property
    def conn(self) -> DBConn:
        return self._conn

    @abc.abstractmethod
    def _record_to_model(self, record: Optional[RowProxy]) -> _STORED_TV:
        pass

    @abc.abstractmethod
    def _model_to_record(self, model: _STORED_TV, for_create: bool) -> Dict[str, Any]:
        pass

    @abc.abstractmethod
    def _model_to_notification_event(self, model: _STORED_TV) -> Dict[str, Any]:
        pass

    def _prepare_model_for_create(self, model: _STORED_TV) -> _STORED_TV:
        return model.clone(id=uuid.uuid4())

    def _prepare_model_for_update(self, model: _STORED_TV) -> _STORED_TV:
        return model

    async def _notify_update(self, model: _STORED_TV) -> None:
        try:
            notification_body = self._model_to_notification_event(model)

            await self._conn.publish(self.update_notification_channel, notification_body)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa
            LOGGER.exception("Notification failure for channel %r", self.update_notification_channel)

    async def create(self, model: _STORED_TV) -> _STORED_TV:
        assert model.id is None
        model_to_create = self._prepare_model_for_create(model)

        await self._conn.execute(self.record_cls.insert_().values(
            **self._model_to_record(model_to_create, for_create=True)
        ))
        await self._notify_update(model_to_create)
        return model_to_create

    async def update(self, model: _STORED_TV) -> _STORED_TV:
        assert model.id is not None
        model_to_update = self._prepare_model_for_update(model)

        await self._conn.execute(
            self.record_cls.update_()
                .values(**self._model_to_record(model_to_update, for_create=False))
                .where(self.record_cls.id == model.id)
        )
        await self._notify_update(model_to_update)
        return model_to_update

    async def get_by_id(self, the_id: uuid.UUID, for_update: bool = False) -> Optional[_STORED_TV]:
        rec_cls = self.record_cls

        query = rec_cls.select_().where(rec_cls.id == the_id)
        if for_update:
            assert self.conn.in_transaction
            query = query.with_for_update()

        result = await self._conn.execute(query)
        row = await result.fetchone()
        result.close()

        return self._record_to_model(row)

    async def delete_by_id(self, the_id: uuid.UUID) -> None:
        rec_cls = self.record_cls

        await self._conn.execute(
            rec_cls.delete_().where(rec_cls.id == the_id)
        )
