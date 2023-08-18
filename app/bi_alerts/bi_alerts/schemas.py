from __future__ import annotations

import marshmallow as ma
from marshmallow_enum import EnumField

from .enums import NotificationTransportType, AlertType


class BINotificationRecipientSchema(ma.Schema):
    email = ma.fields.String()


class BINotificationSchema(ma.Schema):
    recipient = ma.fields.Nested(BINotificationRecipientSchema, required=True)
    transport = EnumField(NotificationTransportType, required=True)


class BIChartLineSchema(ma.Schema):
    type = ma.fields.String()
    yaxis = ma.fields.Integer()
    name = ma.fields.String()


class BIAlertParamsSchema(ma.Schema):
    alarm = ma.fields.List(ma.fields.Dict())


class BIAlertSchema(ma.Schema):
    id = ma.fields.String()
    name = ma.fields.String(required=True)
    description = ma.fields.String()
    type = EnumField(AlertType, required=True)
    window = ma.fields.Integer()
    aggregation = ma.fields.String()
    params = ma.fields.Nested(BIAlertParamsSchema, required=True)


class BIAlertCreateRequestSchema(ma.Schema):
    chart_id = ma.fields.String(required=True)
    oauth = ma.fields.String()
    chart_params = ma.fields.Dict(dump_default=dict())
    chart_lines = ma.fields.List(ma.fields.Nested(BIChartLineSchema), required=True)
    alert = ma.fields.Nested(BIAlertSchema, required=True)
    notifications = ma.fields.List(ma.fields.Nested(BINotificationSchema), required=True)
    time_shift = ma.fields.Integer(load_default=0)


class BIAlertCreateResponseSchema(ma.Schema):
    id = ma.fields.String()


class BIAlertGetSchema(ma.Schema):
    alert_id = ma.fields.String()


class BIAlertsRequestSchema(ma.Schema):
    chart_id = ma.fields.String(required=True)


class BIAlertResponseSchema(ma.Schema):
    alert = ma.fields.Nested(BIAlertSchema)
    chart_lines = ma.fields.List(ma.fields.Nested(BIChartLineSchema))
    chart_params = ma.fields.Dict()
    notifications = ma.fields.List(ma.fields.Nested(BINotificationSchema))
    time_shift = ma.fields.Integer()


class BIChartDataSchema(ma.Schema):
    service_name = ma.fields.String(required=True)


class BIAlertCheckResponseSchema(ma.Schema):
    has_alerts = ma.fields.Boolean()
