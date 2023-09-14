from typing import Optional

from marshmallow import fields as ma_fields

from bi_api_connector.api_schema.extras import FieldExtra


def cache_ttl_field(
    attribute: str,
    required: bool = False,
    allow_none: bool = True,
    missing: Optional[int] = None,
    default: Optional[int] = None,
    bi_extra: FieldExtra = FieldExtra(editable=True),
) -> ma_fields.Integer:
    return ma_fields.Integer(
        attribute=attribute,
        required=required,
        allow_none=allow_none,
        load_default=missing,
        dump_default=default,
        bi_extra=bi_extra,
    )


def secret_string_field(
    attribute: str,
    required: bool = True,
    allow_none: bool = False,
    default: Optional[str] = None,
    bi_extra: FieldExtra = FieldExtra(editable=True),
) -> ma_fields.String:
    return ma_fields.String(
        attribute=attribute,
        load_only=True,  # do not expose never
        required=required,
        allow_none=allow_none,
        dump_default=default,
        bi_extra=bi_extra,
    )
