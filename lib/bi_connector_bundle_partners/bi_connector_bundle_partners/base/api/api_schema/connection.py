from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    Type,
)

from marshmallow import ValidationError

from dl_api_connector.api_schema.connection_base import (
    ConnectionMetaMixin,
    ConnectionSchema,
)
from dl_api_connector.api_schema.connection_base_fields import secret_string_field
from dl_api_connector.api_schema.extras import FieldExtra
from dl_core.flask_utils.us_manager_middleware import USManagerFlaskMiddleware


if TYPE_CHECKING:
    from bi_connector_bundle_partners.base.core.us_connection import PartnersCHConnectionBase


class PartnersConnectionSchemaBase(ConnectionMetaMixin, ConnectionSchema):
    TARGET_CLS: ClassVar[Type[PartnersCHConnectionBase]]

    access_token = secret_string_field(
        attribute="data.access_token",
        bi_extra=FieldExtra(editable=[]),
    )

    def create_data_model_constructor_kwargs(self, data_attributes: Dict[str, Any]) -> Dict[str, Any]:
        base_kwargs = super().create_data_model_constructor_kwargs(data_attributes)
        access_token = base_kwargs.pop("access_token")

        try:
            token_data = self.TARGET_CLS.decrypt_access_token(
                access_token,
                usm=USManagerFlaskMiddleware.get_request_us_manager(),
            )
        except ValueError as ex:
            raise ValidationError("Invalid access token.") from ex

        return dict(
            base_kwargs,
            db_name=token_data.get("db_name"),
        )
