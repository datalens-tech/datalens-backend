import marshmallow as ma

from dl_auth_api_lib.schemas.base import BaseRequestSchema


class YandexUriRequestSchema(BaseRequestSchema):
    pass


class YandexTokenRequestSchema(BaseRequestSchema):
    code = ma.fields.String(required=True)


class YandexUriResponseSchema(ma.Schema):
    uri = ma.fields.String()


class YandexTokenResponseSchema(ma.Schema):
    token_type = ma.fields.String()
    access_token = ma.fields.String()
    expires_in = ma.fields.Integer()
    refresh_token = ma.fields.String()
    scope = ma.fields.String()


class YandexTokenErrorResponseSchema(ma.Schema):
    error_description = ma.fields.String()
    error = ma.fields.String()
