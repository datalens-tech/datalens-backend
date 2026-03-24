from __future__ import annotations

from typing import Any

from marshmallow import fields as ma_fields

from dl_api_lib.request_model.data import DataRequestModel
from dl_api_lib.schemas.action import ActionSchema
from dl_api_lib.schemas.dataset_base import DatasetContentInternalSchema
from dl_model_tools.schema.base import (
    BaseSchema,
    DefaultSchema,
)


class CacheInvalidationTestRequestSchema(DefaultSchema[DataRequestModel]):
    """
    Request schema for cache invalidation test endpoint.

    Similar to preview — accepts the full dataset in the body.

    This request does not affect the main cache and does not take into account
    the configured invalidation cache throttling. It is intended for debugging
    the invalidation query by the user.
    """

    TARGET_CLS = DataRequestModel

    dataset = ma_fields.Nested(DatasetContentInternalSchema, required=False)
    updates = ma_fields.Nested(ActionSchema, many=True, load_default=[])

    def to_object(self, data: dict[str, Any]) -> DataRequestModel:
        return DataRequestModel(
            dataset=data.get("dataset"),
            updates=data.get("updates", []),
        )


class CacheInvalidationTestResultSchema(BaseSchema):
    """Nested result object in success response."""

    value = ma_fields.String(required=True)
    query = ma_fields.String(required=True)


class CacheInvalidationTestResponseSchema(BaseSchema):
    """
    Response schema for successful cache invalidation test (200).

    Example::

        {
          "result": {
            "value": "abcd",
            "query": "SELECT field FROM table WHERE year = '2024'"
          }
        }

    Error responses (400) are handled by the standard error handling middleware`.
    """

    result = ma_fields.Nested(CacheInvalidationTestResultSchema, required=True)
