from __future__ import annotations

import logging
import json

from aiohttp import web
from datetime import datetime

from bi_alerts.utils.solomon import SolomonChannel, SolomonAlert
from bi_alerts.utils.charts import hash_dict, hash_str
from bi_alerts.utils.blackbox import (
    check_oauth_token, check_cookies, is_dl_superuser
)

from .. import schemas as s
from ..db_manager import (
    BIAlertManager,
    BIDatasyncManager,
    BIDatasyncAlertManager,
    BIAlertJoinManager,
    BINotificationManager,
    BIAlertNotificationManager,
)


LOGGER = logging.getLogger(__name__)


class AlertView(web.View):
    async def post(self):  # type: ignore  # TODO: fix
        data = s.BIAlertCreateRequestSchema().load(await self.request.json())
        LOGGER.info(
            'Going to create BIAlert, params: %s',
            {k: v for k, v in data.items() if k not in ('oauth',)},
        )
        auth_result = await check_oauth_token(
            data['oauth'],
            self.request.remote,  # type: ignore  # TODO: fix
            self.request.host,
        )
        if auth_result['username'] is None:
            blackbox_response = auth_result.get('blackbox_response')
            LOGGER.warning("Blackbox auth was not passed. Blackbox resp: %s", json.dumps(
                blackbox_response
            ))
            raise web.HTTPForbidden(reason=blackbox_response.get('error'))  # type: ignore  # TODO: fix

        encrypted_data = self.request.app.crypto.encrypt_with_actual_key(data['oauth'])  # type: ignore  # TODO: fix

        async with self.request.app.db.acquire() as conn:  # type: ignore  # TODO: fix
            async with conn.begin():
                datasync_id = await BIDatasyncManager(conn).create_if_not_exists(
                    chart_id=data['chart_id'],
                    chart_params=data['chart_params'],
                    oauth=data['oauth'],
                    oauth_encrypted=encrypted_data.get('cypher_text'),
                    crypto_key_id=encrypted_data.get('key_id'),
                    time_shift=data['time_shift'],
                )
                solomon_alert = SolomonAlert(
                    name=data['alert']['name'],
                    description=data['alert'].get('description', ''),
                    window=data['alert']['window'],
                    aggregation=data['alert']['aggregation'],
                    alert_type=data['alert']['type'],
                    alert_params=data['alert']['params'],
                    selectors={
                        'chart_id': data['chart_id'],
                        'hashed_params': hash_dict(data['chart_params']),
                        'shard_id': hash_str(data['chart_id'])[:2],
                        'lines': data['chart_lines'],
                    },
                    annotations={
                        'chart_id': data['chart_id'],
                        'chart_params': json.dumps(data['chart_params']),
                        'time_shift': data['time_shift'],
                    },
                )
                alert_id = await BIAlertManager(conn).create(
                    name=data['alert']['name'],
                    description=data['alert'].get('description'),
                    alert_method=data['alert']['type'],
                    alert_params=data['alert']['params'],
                    lines=data['chart_lines'],
                    owner=auth_result.get('username', ''),
                    owner_uid=auth_result.get('user_id'),
                    window=data['alert']['window'],
                    aggregation=data['alert']['aggregation'],
                    external_id=solomon_alert.id,
                )
                for notification in data['notifications']:
                    solomon_channel = SolomonChannel(
                        method=notification['transport'].name,
                        recipient=notification['recipient'][notification['transport'].name],
                    )
                    notification_id = await BINotificationManager(conn).create_if_not_exists(
                        recipient=notification['recipient'][notification['transport'].name],
                        transport=notification['transport'],
                        external_id=solomon_channel.id,
                    )
                    await self.request.app.solomon.create_channel_if_not_exists(solomon_channel)  # type: ignore  # TODO: fix
                    await BIAlertNotificationManager(conn).create(
                        alert_id=alert_id,
                        notification_id=notification_id,
                    )
                    solomon_alert.add_channel(solomon_channel)
                await self.request.app.solomon.create_alert(solomon_alert)  # type: ignore  # TODO: fix
                await BIDatasyncAlertManager(conn).create(
                    datasync_id=datasync_id,
                    alert_id=alert_id,
                )
        return web.json_response(s.BIAlertCreateResponseSchema().dump({'id': str(alert_id)}))


class AlertInfoView(web.View):
    async def get(self):  # type: ignore  # TODO: fix
        data = s.BIAlertGetSchema().load(self.request.match_info)
        async with self.request.app.db.acquire() as conn:  # type: ignore  # TODO: fix
            alert = await BIAlertManager(conn).get_by_id(
                id_=int(data['alert_id']),
            )
            datasync_alert = await BIDatasyncAlertManager(conn).get_alert_datasync(
                alert_id=alert.id,
            )
            datasync = await BIDatasyncManager(conn).get_by_id(
                id_=datasync_alert.datasync_id,  # type: ignore  # TODO: fix
            )
            notifications = await BIAlertJoinManager(conn).get_alert_notification(
                alert_id=alert.id
            )

        return web.json_response(s.BIAlertResponseSchema().dump({
            'alert': {
                'id': str(alert.id),
                'name': alert.name,
                'description': alert.description,
                'type': alert.alert_method,
                'params': alert.alert_params,
                'window': alert.window,
                'aggregation': alert.aggregation,
            },
            'chart_lines': alert.lines,
            'chart_params': datasync.chart_params,
            'notifications': [
                {
                    'transport': notification.bi_notification_transport,
                    'recipient': {
                        notification.bi_notification_transport.name:
                            notification.bi_notification_recipient,
                    },
                } for notification in notifications  # type: ignore  # TODO: fix
            ],
            'time_shift': datasync.time_shift,
        }))

    async def delete(self):  # type: ignore  # TODO: fix
        data = s.BIAlertGetSchema().load(self.request.match_info)
        alert_id = int(data['alert_id'])

        LOGGER.info('Going to delete alert %s', alert_id)

        async with self.request.app.db.acquire() as conn:  # type: ignore  # TODO: fix
            async with conn.begin():
                datasync_alert = await BIDatasyncAlertManager(conn).get_alert_datasync(
                    alert_id=alert_id,
                )
                await BIDatasyncAlertManager(conn).delete_by_id(datasync_alert.id)  # type: ignore  # TODO: fix

                alert_notifications = await BIAlertNotificationManager(
                    conn
                ).get_alert_notifications(
                    alert_id=alert_id,
                )
                for alert_notification in alert_notifications:  # type: ignore  # TODO: fix
                    await BIAlertNotificationManager(conn).delete_by_id(alert_notification.id)
                alert = await BIAlertManager(conn).get_by_id(alert_id)
                await self.request.app.solomon.delete_alert(alert.external_id)  # type: ignore  # TODO: fix
                await BIAlertManager(conn).delete_by_id(alert_id)

        return web.Response()

    async def put(self):  # type: ignore  # TODO: fix
        alert_id = int(s.BIAlertGetSchema().load(self.request.match_info)['alert_id'])
        data = s.BIAlertCreateRequestSchema().load(await self.request.json())
        auth_result = await check_cookies(
            self.request.cookies,
            self.request.remote,  # type: ignore  # TODO: fix
            self.request.host,
        )
        if auth_result['username'] is None:
            blackbox_response = auth_result.get('blackbox_response')
            LOGGER.warning("Blackbox auth was not passed. Blackbox resp: %s", json.dumps(
                blackbox_response
            ))
            raise web.HTTPForbidden(reason=blackbox_response.get('error'))  # type: ignore  # TODO: fix

        utc_datetime = datetime.utcnow()
        LOGGER.info(
            'Going to update BIAlert %s, params: %s',
            alert_id, {k: v for k, v in data.items() if k not in ('oauth',)},
        )

        async with self.request.app.db.acquire() as conn:  # type: ignore  # TODO: fix
            async with conn.begin():
                alert = await BIAlertManager(conn).get_by_id(alert_id)
                datasync_alert = await BIDatasyncAlertManager(conn).get_alert_datasync(
                    alert_id=alert.id,
                )
                external_solomon_alert = await self.request.app.solomon.get_alert(  # type: ignore  # TODO: fix
                    alert.external_id
                )
                solomon_alert = SolomonAlert(
                    name=data['alert']['name'],
                    description=data['alert'].get('description', ''),
                    window=data['alert']['window'],
                    aggregation=data['alert']['aggregation'],
                    alert_type=data['alert']['type'],
                    alert_params=data['alert']['params'],
                    selectors={
                        'chart_id': data['chart_id'],
                        'hashed_params': hash_dict(data['chart_params']),
                        'shard_id': hash_str(data['chart_id'])[:2],
                        'lines': data['chart_lines'],
                    },
                    annotations={
                        'chart_id': data['chart_id'],
                        'chart_params': json.dumps(data['chart_params']),
                        'time_shift': data['time_shift'],
                    },
                    id=alert.external_id,
                )
                await BIAlertManager(conn).update(
                    alert_id,
                    name=data['alert']['name'],
                    description=data['alert'].get('description'),
                    alert_method=data['alert']['type'],
                    alert_params=data['alert']['params'],
                    lines=data['chart_lines'],
                    owner=auth_result.get('username', ''),
                    owner_uid=auth_result.get('user_id'),
                    window=data['alert']['window'],
                    aggregation=data['alert']['aggregation'],
                    update_time=utc_datetime,
                )
                await BIDatasyncManager(conn).update(
                    datasync_alert.datasync_id,  # type: ignore  # TODO: fix
                    time_shift=data['time_shift'],
                )
                old_notifications = await BIAlertJoinManager(conn).get_alert_notification(
                    alert_id=alert_id,
                )
                old_notifications_ids = {
                    notification.bi_notification_id for notification in old_notifications  # type: ignore  # TODO: fix
                }
                for notification in data['notifications']:
                    solomon_channel = SolomonChannel(
                        method=notification['transport'].name,
                        recipient=notification['recipient'][notification['transport'].name],
                    )
                    solomon_alert.add_channel(solomon_channel)
                    notification_id = await BINotificationManager(conn).create_if_not_exists(
                        recipient=notification['recipient'][notification['transport'].name],
                        transport=notification['transport'],
                        external_id=solomon_channel.id,
                    )
                    await self.request.app.solomon.create_channel_if_not_exists(solomon_channel)  # type: ignore  # TODO: fix
                    if notification_id not in old_notifications_ids:
                        await BIAlertNotificationManager(conn).create(
                            alert_id=alert_id,
                            notification_id=notification_id,
                        )
                    else:
                        old_notifications_ids.remove(notification_id)
                for notification_id in old_notifications_ids:
                    alert_notification = await BIAlertNotificationManager(conn).get(
                        alert_id=alert_id,
                        notification_id=notification_id,
                    )
                    await BIAlertNotificationManager(conn).delete_by_id(alert_notification.id)  # type: ignore  # TODO: fix
                await self.request.app.solomon.update_alert(  # type: ignore  # TODO: fix
                    solomon_alert.id, solomon_alert, external_solomon_alert['version'],
                )

        return web.Response()


class AlertsListView(web.View):
    async def post(self):  # type: ignore  # TODO: fix
        data = s.BIAlertsRequestSchema().load(await self.request.json())
        auth_result = await check_cookies(
            self.request.cookies,
            self.request.remote,  # type: ignore  # TODO: fix
            self.request.host,
        )

        if is_dl_superuser(auth_result, self.request.headers):
            auth_result['username'] = None

        async with self.request.app.db.acquire() as conn:  # type: ignore  # TODO: fix
            alerts = await BIAlertJoinManager(conn).get_datasync_alerts_for_chart(
                chart_id=data['chart_id'],
                owner=auth_result.get('username'),
            )
            notifications = [
                await BIAlertJoinManager(conn).get_alert_notification(
                    alert.bi_alert_id
                ) for alert in alerts  # type: ignore  # TODO: fix
            ]

        return web.json_response({
            'alerts': [
                s.BIAlertResponseSchema().dump({
                    'alert': {
                        'id': str(alert.bi_alert_id),
                        'name': alert.bi_alert_name,
                        'description': alert.bi_alert_description,
                        'type': alert.bi_alert_alert_method,
                        'params': alert.bi_alert_alert_params,
                        'window': alert.bi_alert_window,
                        'aggregation': alert.bi_alert_aggregation,
                    },
                    'chart_lines': alert.bi_alert_lines,
                    'chart_params': alert.bi_datasync_chart_params,
                    'notifications': [
                        {
                            'transport': elem.bi_notification_transport,
                            'recipient': {
                                elem.bi_notification_transport.name:
                                    elem.bi_notification_recipient,
                            },
                        } for elem in notification  # type: ignore  # TODO: fix
                    ],
                    'time_shift': alert.bi_datasync_time_shift,
                }) for alert, notification in zip(alerts, notifications)  # type: ignore  # TODO: fix
            ],
        })


class AlertsCheckView(web.View):
    async def post(self):  # type: ignore  # TODO: fix
        data = s.BIAlertsRequestSchema().load(await self.request.json())

        async with self.request.app.db.acquire() as conn:  # type: ignore  # TODO: fix
            alerts = await BIAlertJoinManager(conn).get_datasync_alerts_for_chart(
                chart_id=data['chart_id'],
            )

        return web.json_response(
            s.BIAlertCheckResponseSchema().dump({
                'has_alerts': bool(alerts),
            })
        )
