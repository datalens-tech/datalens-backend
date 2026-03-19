from marshmallow import fields as ma_fields

from dl_constants.enums import OrderDirection
from dl_core.fields import OrderField
from dl_model_tools.schema.base import DefaultSchema


class OrderFieldSchema(DefaultSchema[OrderField]):
    TARGET_CLS = OrderField

    id = ma_fields.String(required=True)
    guid = ma_fields.String()
    order = ma_fields.Enum(OrderDirection, load_default=OrderDirection.asc)
    valid = ma_fields.Boolean(load_default=True)
