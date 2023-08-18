from __future__ import annotations

import logging
from abc import ABC
from datetime import datetime
from typing import TYPE_CHECKING, ClassVar, List, Optional, Type

import sqlalchemy as sa

from . import exc
from .models.db import BIAlert, BIDatasync, BINotification, AlertNotification, DatasyncAlert
from .enums import AlertType, NotificationTransportType
from .utils.charts import hash_dict, hash_str

if TYPE_CHECKING:
    from aiopg.sa import SAConnection

    from .models import Base as BaseModel

LOGGER = logging.getLogger(__name__)


def _legacy_labels(*tables):  # type: ignore  # TODO: fix
    result = []
    for table in tables:
        for column in table.c:
            result.append(column.label(f'{table.name}_{column.name}'))
    return result


class DbManager(ABC):
    _model: ClassVar[Type[BaseModel]]
    _whitelist = set()  # type: ignore  # TODO: fix

    def __init__(self, db_conn: SAConnection):
        self.db_conn = db_conn

    async def get_by_id(self, id_: int):  # type: ignore  # TODO: fix
        result = await self.db_conn.execute(
            self._model.select_().where(  # type: ignore  # TODO: fix
                self._model.id == id_,  # type: ignore  # TODO: fix
            )
        )
        db_obj = await result.fetchone()
        if not db_obj:
            raise exc.NotFoundError()

        return db_obj

    async def delete_by_id(self, id_: int):  # type: ignore  # TODO: fix
        await self.db_conn.execute(
            self._model.delete_().where(  # type: ignore  # TODO: fix
                self._model.id == id_,  # type: ignore  # TODO: fix
            )
        )
        LOGGER.info('Object from %s deleted, id: %s', self._model.__tablename__, id_)  # type: ignore  # TODO: fix

    async def update(self, id_: int, **updates):  # type: ignore  # TODO: fix
        assert set(updates.keys()) <= self._whitelist
        await self.db_conn.execute(
            self._model.update_()  # type: ignore  # TODO: fix
            .where(self._model.id == id_)  # type: ignore  # TODO: fix
            .values(**updates)
        )
        LOGGER.info('Object from %s updated, id: %s', self._model.__tablename__, id_)  # type: ignore  # TODO: fix


class BIAlertManager(DbManager):
    _model: ClassVar[Type[BIAlert]] = BIAlert
    _whitelist = {
        'name',
        'description',
        'alert_method',
        'alert_params',
        'lines',
        'owner',
        'owner_uid',
        'window',
        'aggregation',
        'update_time',
    }

    async def create(
        self,
        name: str,
        alert_method: AlertType,
        alert_params: dict,
        lines: List[dict],
        owner: str,
        owner_uid: Optional[str],
        window: int,
        aggregation: str,
        external_id: str,
        description: Optional[str] = None,
    ) -> int:
        utc_datetime = datetime.utcnow()

        id_ = await self.db_conn.scalar(
            self._model.insert_().values(  # type: ignore  # TODO: fix
                name=name,
                description=description,
                alert_method=alert_method,
                alert_params=alert_params,
                lines=lines,
                owner=owner,
                owner_uid=owner_uid,
                window=window,
                aggregation=aggregation,
                external_id=external_id,
                creation_time=utc_datetime,
                update_time=utc_datetime,
            )
        )

        LOGGER.info('DLAlert created, id: %s', id_)
        return id_

    async def get_by_owner(self, owner: str) -> Optional[List[BIAlert]]:
        result = await self.db_conn.execute(
            self._model.select_().where(  # type: ignore  # TODO: fix
                self._model.owner == owner,  # type: ignore  # TODO: fix
            )
        )

        return await result.fetchall()


class BIDatasyncManager(DbManager):
    _model: ClassVar[Type[BIDatasync]] = BIDatasync
    _whitelist = {
        'last_check_time',
        'active',
        'time_shift',
    }

    async def create(
        self,
        chart_id: str,
        chart_params: dict,
        oauth: Optional[str] = None,
        oauth_encrypted: Optional[str] = None,
        crypto_key_id: Optional[str] = None,
        time_shift: int = 0,
    ) -> int:
        chart_params_hash = hash_dict(chart_params)
        external_shard_id = hash_str(chart_id)[:2]
        oauth_hash = hash_str(oauth) if oauth is not None else None
        utc_datetime = datetime.utcnow()

        id_ = await self.db_conn.scalar(
            self._model.insert_().values(  # type: ignore  # TODO: fix
                chart_id=chart_id,
                chart_params=chart_params,
                chart_params_hash=chart_params_hash,
                oauth_hash=oauth_hash,
                oauth_encrypted=oauth_encrypted,
                crypto_key_id=crypto_key_id,
                time_shift=time_shift,
                external_shard_id=external_shard_id,
                creation_time=utc_datetime,
                last_check_time=utc_datetime,
            )
        )

        LOGGER.info('BIDatasync created, id: %s', id_)
        return id_

    async def create_if_not_exists(
        self,
        chart_id: str,
        chart_params: dict,
        oauth: Optional[str] = None,
        oauth_encrypted: Optional[str] = None,
        crypto_key_id: Optional[str] = None,
        time_shift: int = 0,
    ) -> int:
        datasync = await self.get(chart_id, chart_params, oauth)
        if datasync:
            LOGGER.info('BIDatasync already exists, id: %s', datasync.id)
            return datasync.id
        id_ = await self.create(
            chart_id, chart_params, oauth, oauth_encrypted, crypto_key_id, time_shift,
        )
        return id_

    async def get(
        self,
        chart_id: str,
        chart_params: dict,
        oauth: Optional[str] = None,
    ) -> Optional[BIDatasync]:
        chart_params_hash = hash_dict(chart_params)
        oauth_hash = hash_str(oauth) if oauth is not None else None
        result = await self.db_conn.execute(
            self._model.select_().where(  # type: ignore  # TODO: fix
                (self._model.chart_id == chart_id)  # type: ignore  # TODO: fix
                & (self._model.chart_params_hash == chart_params_hash)  # type: ignore  # TODO: fix
                & (self._model.oauth_hash == oauth_hash)  # type: ignore  # TODO: fix
            )
        )

        return await result.fetchone()

    async def get_top_for_service(
        self,
        service: str,
        limit: int = 10,
    ) -> Optional[List[BIDatasync]]:
        result = await self.db_conn.execute(
            self._model.select_().where(  # type: ignore  # TODO: fix
                self._model.external_shard_id == service,  # type: ignore  # TODO: fix
            ).order_by(
                self._model.last_check_time  # type: ignore  # TODO: fix
            )
        )

        return await result.fetchmany(size=limit)


class BINotificationManager(DbManager):
    _model: ClassVar[type[BINotification]] = BINotification

    async def create(
        self,
        recipient: str,
        transport: NotificationTransportType,
        external_id: str,
    ) -> int:
        id_ = await self.db_conn.scalar(
            self._model.insert_().values(  # type: ignore  # TODO: fix
                recipient=recipient,
                transport=transport,
                external_id=external_id,
            )
        )

        LOGGER.info('BINotification created, id: %s', id_)
        return id_

    async def create_if_not_exists(
        self,
        recipient: str,
        transport: NotificationTransportType,
        external_id: str,
    ) -> int:
        notification = await self.get(recipient, transport)
        if notification:
            LOGGER.info('BINotification already exists, id: %s', notification.id)
            return notification.id
        id_ = await self.create(recipient, transport, external_id)
        return id_

    async def get(
        self,
        recipient: str,
        transport: NotificationTransportType,
    ) -> Optional[BINotification]:
        result = await self.db_conn.execute(
            self._model.select_().where(  # type: ignore  # TODO: fix
                (self._model.recipient == recipient)  # type: ignore  # TODO: fix
                & (self._model.transport == transport)  # type: ignore  # TODO: fix
            )
        )

        return await result.fetchone()


class BIAlertNotificationManager(DbManager):
    _model: ClassVar[Type[AlertNotification]] = AlertNotification

    async def create(
        self,
        alert_id: int,
        notification_id: int,
    ) -> int:
        id_ = await self.db_conn.scalar(
            self._model.insert_().values(  # type: ignore  # TODO: fix
                alert_id=alert_id,
                notification_id=notification_id,
            )
        )

        LOGGER.info('AlertNotification created, id: %s', id_)
        return id_

    async def get_alert_notifications(
        self,
        alert_id: int,
    ) -> Optional[List[AlertNotification]]:
        result = await self.db_conn.execute(
            self._model.select_().where(  # type: ignore  # TODO: fix
                self._model.alert_id == alert_id,  # type: ignore  # TODO: fix
            )
        )
        data = await result.fetchall()

        if not data:
            raise exc.NotFoundError()

        return data

    async def get(
        self,
        alert_id: int,
        notification_id: int,
    ) -> Optional[AlertNotification]:
        result = await self.db_conn.execute(
            self._model.select_().where(  # type: ignore  # TODO: fix
                (self._model.alert_id == alert_id)  # type: ignore  # TODO: fix
                & (self._model.notification_id == notification_id)  # type: ignore  # TODO: fix
            )
        )
        data = await result.fetchone()

        if not data:
            raise exc.NotFoundError()

        return data


class BIDatasyncAlertManager(DbManager):
    _model: ClassVar[Type[DatasyncAlert]] = DatasyncAlert
    _whitelist = {
        'active',
    }

    async def create(
        self,
        datasync_id: int,
        alert_id: int,
    ) -> int:
        id_ = await self.db_conn.scalar(
            self._model.insert_().values(  # type: ignore  # TODO: fix
                datasync_id=datasync_id,
                alert_id=alert_id,
            )
        )

        LOGGER.info('DatasyncAlert created, id: %s', id_)
        return id_

    async def get_datasync_alerts(
        self,
        datasync_id: int,
    ) -> Optional[List[DatasyncAlert]]:
        result = await self.db_conn.execute(
            self._model.select_().where(  # type: ignore  # TODO: fix
                self._model.datasync_id == datasync_id,  # type: ignore  # TODO: fix
            )
        )
        data = await result.fetchall()

        if not data:
            raise exc.NotFoundError()

        return data

    async def get_alert_datasync(
        self,
        alert_id: int,
    ) -> Optional[DatasyncAlert]:
        result = await self.db_conn.execute(
            self._model.select_().where(  # type: ignore  # TODO: fix
                self._model.alert_id == alert_id,  # type: ignore  # TODO: fix
            )
        )
        data = await result.fetchone()

        if not data:
            raise exc.NotFoundError()

        return data


class BIAlertJoinManager(DbManager):
    _model_da = DatasyncAlert
    _model_an = AlertNotification
    _model_d = BIDatasync
    _model_a = BIAlert
    _model_n = BINotification

    async def get_datasync_alerts_for_chart(
        self,
        chart_id: str,
        owner: Optional[str] = None,
    ) -> Optional[List[BIAlert, BIDatasync]]:  # type: ignore  # TODO: fix
        query = self._model_d.__table__.join(
            self._model_da.__table__,
            self._model_d.__table__.c.id == self._model_da.__table__.c.datasync_id,
        )
        query = query.join(
            self._model_a.__table__,
            self._model_a.__table__.c.id == self._model_da.__table__.c.alert_id,
        )
        query = sa.select(
            _legacy_labels(self._model_a.__table__, self._model_d.__table__)
        ).select_from(query).where(
            self._model_d.__table__.c.chart_id == chart_id,
        ).where(
            self._model_da.__table__.c.active,
        )

        if owner is not None:
            query = query.where(
                self._model_a.__table__.c.owner == owner,
            )

        result = await self.db_conn.execute(query.apply_labels())

        return await result.fetchall()

    async def get_alert_notification(
        self,
        alert_id: int,
    ) -> Optional[List[BINotification]]:
        query = self._model_a.__table__.join(
            self._model_an.__table__,
            self._model_a.__table__.c.id == self._model_an.__table__.c.alert_id,
        )
        query = query.join(
            self._model_n.__table__,
            self._model_n.__table__.c.id == self._model_an.__table__.c.notification_id,
        )
        query = sa.select(
            # Normally, `[self._model_n.__table__]` would work,
            # but `aiopg` seems to need more fixing for `sqlalchemy>=1.4`
            _legacy_labels(self._model_n.__table__),
        ).select_from(query).where(
            self._model_a.__table__.c.id == alert_id,
        ).where(
            self._model_an.__table__.c.active,
        )

        query = query.set_label_style(sa.sql.LABEL_STYLE_TABLENAME_PLUS_COL)
        result = await self.db_conn.execute(query)

        return await result.fetchall()
