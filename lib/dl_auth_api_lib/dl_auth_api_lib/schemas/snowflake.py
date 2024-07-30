import string

import marshmallow as ma
from marshmallow import validate as ma_validate

from dl_auth_api_lib.schemas.base import BaseRequestSchema


VALID_SYMBOLS = string.ascii_letters + string.digits + "-_."


class SnowflakeBaseRequestSchema(BaseRequestSchema):
    account = ma.fields.String(
        required=True,
        validate=ma_validate.ContainsOnly(VALID_SYMBOLS, error="Invalid account_name"),
    )
    client_id = ma.fields.String(required=True)


class SnowflakeUriRequestSchema(SnowflakeBaseRequestSchema):
    pass


class SnowflakeTokenRequestSchema(SnowflakeBaseRequestSchema):
    code = ma.fields.String(required=True)
    client_secret = ma.fields.String(required=True)


class SnowflakeUriResponseSchema(ma.Schema):
    uri = ma.fields.String()


class SnowflakeTokenResponseSchema(ma.Schema):
    token_type = ma.fields.String()
    access_token = ma.fields.String()
    expires_in = ma.fields.Integer()
    refresh_token = ma.fields.String()
    username = ma.fields.String()


class SnowflakeTokenErrorResponseSchema(ma.Schema):
    message = ma.fields.String()
    error = ma.fields.String()
