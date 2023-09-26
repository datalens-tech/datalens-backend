from __future__ import annotations

from urllib.parse import urlparse

from marshmallow import fields as ma_fields
from marshmallow import validate as ma_validate

from dl_api_connector.api_schema.connection_base import (
    ConnectionMetaMixin,
    ConnectionSchema,
)
from dl_api_connector.api_schema.connection_base_fields import (
    cache_ttl_field,
    secret_string_field,
)
from dl_api_connector.api_schema.extras import FieldExtra

from bi_connector_bitrix_gds.core.us_connection import BitrixGDSConnection


class BitrixPortalValidator(ma_validate.Validator):
    error = "Not a valid portal name"

    def __call__(self, portal: str) -> str:
        parsed_host = urlparse("//{}".format(portal)).hostname
        if portal.lower() != parsed_host:
            raise ma_validate.ValidationError(self.error)
        return portal


class BitrixGDSConnectionSchema(ConnectionMetaMixin, ConnectionSchema):
    TARGET_CLS = BitrixGDSConnection  # type: ignore

    portal = ma_fields.String(
        attribute="data.portal",
        required=True,
        allow_none=False,
        bi_extra=FieldExtra(editable=True),
        validate=BitrixPortalValidator(),
    )
    token = secret_string_field(attribute="data.token")
    cache_ttl_sec = cache_ttl_field(attribute="data.cache_ttl_sec")
