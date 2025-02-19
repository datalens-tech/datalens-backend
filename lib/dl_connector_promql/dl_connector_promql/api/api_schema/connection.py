from __future__ import annotations

import re
from typing import (
    Any,
    Mapping,
    Optional,
)

from marshmallow import ValidationError
from marshmallow import fields as ma_fields

from dl_api_connector.api_schema.connection_base import ConnectionMetaMixin
from dl_api_connector.api_schema.connection_base_fields import secret_string_field
from dl_api_connector.api_schema.connection_sql import ClassicSQLConnectionSchema
from dl_api_connector.api_schema.extras import FieldExtra

from dl_connector_promql.core.us_connection import PromQLConnection


class DBPathField(ma_fields.String):
    """
    Field that validates url path for prometheus
    Removes starting slash and adds ending slash if it's not presented
    """

    @staticmethod
    def _validate_path_str(
        user_path_str: str,
    ) -> None:
        path_re = re.compile(r"^(([a-zA-Z0-9-_:]+)?(/[a-zA-Z0-9-_:]+)*)/?$")
        if not path_re.match(user_path_str):
            raise ValidationError("Path in the wrong format")

    def _deserialize(self, value: Any, attr: Optional[str], data: Optional[Mapping[str, Any]], **kwargs: Any) -> Any:
        user_path_str = super()._deserialize(value, attr, data, **kwargs)
        self._validate_path_str(user_path_str)
        user_path_str = user_path_str.lstrip("/")
        if not user_path_str.endswith("/"):
            user_path_str += "/"
        return user_path_str


class PromQLConnectionSchema(ConnectionMetaMixin, ClassicSQLConnectionSchema):
    TARGET_CLS = PromQLConnection

    secure = ma_fields.Boolean(attribute="data.secure", bi_extra=FieldExtra(editable=True))
    username = ma_fields.String(
        attribute="data.username",
        required=False,
        allow_none=True,
        dump_default=None,
        bi_extra=FieldExtra(editable=True),
    )
    password = secret_string_field(
        attribute="data.password",
        required=False,
        allow_none=True,
    )
    path = DBPathField(
        attribute="data.path",
        required=False,
        allow_none=True,
        dump_default=None,
        bi_extra=FieldExtra(editable=True),
    )
