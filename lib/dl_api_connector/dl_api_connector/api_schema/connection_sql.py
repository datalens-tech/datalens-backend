from __future__ import annotations

from typing import (
    Any,
    Mapping,
    Optional,
)

import marshmallow
from marshmallow import fields as ma_fields

from dl_api_connector.api_schema.connection_base import ConnectionSchema
from dl_api_connector.api_schema.connection_base_fields import (
    cache_ttl_field,
    secret_string_field,
)
from dl_api_connector.api_schema.extras import FieldExtra
from dl_core.utils import (
    parse_comma_separated_hosts,
    validate_hostname_or_ip_address,
)


class DBHostField(ma_fields.String):
    """
    Field that validates comma-separated list of hosts
    If parent schema is instance of ClassicSQLConnectionSchema,
     schema attribute `ALLOW_MULTI_HOST` will be checked to determine if multiple hosts is allowed.
    Otherwise only single host is allowed.
    """

    @staticmethod
    def _validate_host_str(user_hosts_str: str, allow_multi_host: bool) -> None:
        if allow_multi_host:
            host_list = [host for host in parse_comma_separated_hosts(user_hosts_str) if host]
        else:
            host_list = [user_hosts_str]

        if host_list:
            for user_host in host_list:
                try:
                    validate_hostname_or_ip_address(user_host)
                except ValueError as err:
                    raise marshmallow.ValidationError("Unable to parse host: {}".format(user_host)) from err
        else:
            raise marshmallow.ValidationError("Unable to parse host: {}".format(user_hosts_str))

    def _deserialize(self, value: Any, attr: Optional[str], data: Optional[Mapping[str, Any]], **kwargs: Any) -> Any:
        user_host_str = super()._deserialize(value, attr, data, **kwargs)

        schema = self.parent
        # May be assert?
        if isinstance(schema, ClassicSQLConnectionSchema):
            # FIXME: Remove reference to schema. Replace with mixin or flag
            allow_multi_host = schema.ALLOW_MULTI_HOST
        else:
            allow_multi_host = False

        self._validate_host_str(user_host_str, allow_multi_host=allow_multi_host)

        return user_host_str


def db_name_no_query_params(value: Optional[str]):
    if not value:
        return value
    if '?' in value:
        raise marshmallow.ValidationError(
            f"There must be no query params in field db_name, found: {value}"
        )
    return value


class ClassicSQLConnectionSchema(ConnectionSchema):
    ALLOW_MULTI_HOST = False

    host = DBHostField(attribute="data.host", required=True, bi_extra=FieldExtra(editable=True))
    port = ma_fields.Integer(attribute="data.port", required=True, bi_extra=FieldExtra(editable=True))
    username = ma_fields.String(attribute="data.username", required=True, bi_extra=FieldExtra(editable=True))
    password = secret_string_field(attribute="data.password", bi_extra=FieldExtra(editable=True))
    db_name = ma_fields.String(
        attribute="data.db_name",
        allow_none=True,
        bi_extra=FieldExtra(editable=True),
        validate=db_name_no_query_params
    )
    cache_ttl_sec = cache_ttl_field(attribute="data.cache_ttl_sec")
