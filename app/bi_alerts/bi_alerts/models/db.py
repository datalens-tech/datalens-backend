from __future__ import annotations

import datetime

import sqlalchemy as sa

from ..enums import AlertType, NotificationTransportType

from . import Base


class BIAlert(Base):
    __tablename__ = 'bi_alert'

    id = sa.Column(sa.Integer, primary_key=True)

    name = sa.Column(sa.String(512), nullable=False)
    description = sa.Column(sa.String(2048))

    alert_method = sa.Column(sa.Enum(AlertType), nullable=False)
    alert_params = sa.Column(sa.JSON, nullable=False)
    lines = sa.Column(sa.JSON, nullable=False)
    window = sa.Column(sa.Integer)
    aggregation = sa.Column(sa.String(32))

    owner = sa.Column(sa.String(256), nullable=False, index=True)
    owner_uid = sa.Column(sa.String(64))

    external_id = sa.Column(sa.String(64), nullable=False)

    creation_time = sa.Column(sa.DateTime(), default=datetime.datetime.utcnow())
    update_time = sa.Column(sa.DateTime())

    active = sa.Column(sa.Boolean, index=True, nullable=False, default=True)


class BIDatasync(Base):
    __tablename__ = 'bi_datasync'

    id = sa.Column(sa.Integer, primary_key=True)

    chart_id = sa.Column(sa.String(64), nullable=False, index=True)
    chart_params = sa.Column(sa.JSON, nullable=False)
    chart_params_hash = sa.Column(sa.String(64), nullable=False)
    oauth_hash = sa.Column(sa.String(64))
    oauth_encrypted = sa.Column(sa.String(256))
    crypto_key_id = sa.Column(sa.String(64))

    time_shift = sa.Column(sa.Integer, default=0)

    external_shard_id = sa.Column(sa.String(8), nullable=False, index=True)

    creation_time = sa.Column(sa.DateTime(), default=datetime.datetime.utcnow())
    last_check_time = sa.Column(sa.DateTime())

    active = sa.Column(sa.Boolean, index=True, nullable=False, default=True)


class BINotification(Base):
    __tablename__ = 'bi_notification'

    id = sa.Column(sa.Integer, primary_key=True)

    recipient = sa.Column(sa.String(256), nullable=False, index=True)
    transport = sa.Column(sa.Enum(NotificationTransportType), nullable=False)

    external_id = sa.Column(sa.String(64), nullable=False)


class DatasyncAlert(Base):
    __tablename__ = 'datasync_alert'

    id = sa.Column(sa.Integer, primary_key=True)

    datasync_id = sa.Column(
        sa.Integer, sa.ForeignKey(BIDatasync.id, name='fk_datasync_id'),
        index=True)
    alert_id = sa.Column(
        sa.Integer, sa.ForeignKey(BIAlert.id, name='fk_alert_id'),
        index=True)

    active = sa.Column(sa.Boolean, index=True, nullable=False, default=True)


class AlertNotification(Base):
    __tablename__ = 'alert_notification'

    id = sa.Column(sa.Integer, primary_key=True)

    alert_id = sa.Column(
        sa.Integer, sa.ForeignKey(BIAlert.id, name='fk_alert_id'),
        index=True)
    notification_id = sa.Column(
        sa.Integer, sa.ForeignKey(BINotification.id, name='fk_notification_id'),
        index=True)

    active = sa.Column(sa.Boolean, index=True, nullable=False, default=True)
