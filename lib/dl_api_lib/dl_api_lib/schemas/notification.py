from __future__ import annotations

from marshmallow import fields as ma_fields

from dl_constants.enums import NotificationLevel
from dl_model_tools.schema.base import BaseSchema


class NotificationSchema(BaseSchema):
    title = ma_fields.String()
    message = ma_fields.String()
    level = ma_fields.Enum(NotificationLevel)
    locator = ma_fields.String()
