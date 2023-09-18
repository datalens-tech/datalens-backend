from typing import (
    Any,
    Optional,
)

from marshmallow import ValidationError
from marshmallow import fields as ma_fields
from marshmallow import validates_schema

from dl_api_connector.api_schema.connection_base import (
    ConnectionMetaMixin,
    ConnectionSchema,
)
from dl_api_connector.api_schema.connection_base_fields import secret_string_field
from dl_api_connector.api_schema.extras import FieldExtra

from bi_connector_metrica.core.us_connection import (
    AppMetricaApiConnection,
    MetrikaApiConnection,
    parse_metrica_ids,
)


class ConnectionMetrikaLikeAPI(ConnectionMetaMixin, ConnectionSchema):
    token = secret_string_field(attribute="data.token")
    counter_id = ma_fields.String(attribute="data.counter_id", required=True, bi_extra=FieldExtra(editable=True))
    accuracy = ma_fields.Float(
        attribute="data.accuracy",
        allow_none=True,
        dump_default=None,
        load_default=None,
        bi_extra=FieldExtra(editable=True),
    )

    @validates_schema
    def validate_counter_id(self, data: Optional[dict[str, Any]], *args: Any, **kwargs: Any) -> None:
        if data is None or "data" not in data or "counter_id" not in data["data"]:
            return

        ids_orig = data["data"]["counter_id"]
        ids = list(filter(lambda t: t, parse_metrica_ids(ids_orig)))
        if ids:
            for id_str in ids:
                try:
                    id_value = int(id_str)
                except Exception as ex:
                    raise ValidationError(f"Unable to parse id: {id_str!r}") from ex
                if id_value <= 0:
                    raise ValidationError(f"Value should be positive: {id_str!r}")
        else:
            raise ValidationError(f"Unable to parse id: {ids_orig!r}")


class ConnectionMetrikaAPISchema(ConnectionMetrikaLikeAPI):
    TARGET_CLS = MetrikaApiConnection


class ConnectionAppMetricaAPISchema(ConnectionMetrikaLikeAPI):
    TARGET_CLS = AppMetricaApiConnection
