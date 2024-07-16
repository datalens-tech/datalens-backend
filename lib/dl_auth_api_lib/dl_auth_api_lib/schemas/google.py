import marshmallow as ma

from dl_auth_api_lib.schemas.base import BaseRequestSchema


class GoogleUriRequestSchema(BaseRequestSchema):
    pass


class GoogleTokenRequestSchema(BaseRequestSchema):
    code = ma.fields.String(required=True)


class GoogleUriResponseSchema(ma.Schema):
    uri = ma.fields.String()


class GoogleTokenResponseSchema(ma.Schema):
    token_type = ma.fields.String()
    access_token = ma.fields.String()
    expires_in = ma.fields.Integer()
    refresh_token = ma.fields.String()
    scope = ma.fields.String()


class GoogleTokenErrorResponseSchema(ma.Schema):
    error = ma.fields.String()
    error_description = ma.fields.String()
