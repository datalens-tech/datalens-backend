import json
from typing import Any

import pydantic

from dl_cache_engine.cache_invalidation.exc import CacheInvalidationDeserializationError
from dl_cache_engine.cache_invalidation.primitives import (
    CacheInvalidationEntry,
    CacheInvalidationErrorPayload,
    CacheInvalidationStatus,
    CacheInvalidationSuccessPayload,
)


class CacheInvalidationSuccessPayloadSchema(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True)

    data: str

    def to_object(self) -> CacheInvalidationSuccessPayload:
        return CacheInvalidationSuccessPayload(data=self.data)


class CacheInvalidationErrorPayloadSchema(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True)

    error_code: str
    error_message: str | None = None
    error_details: dict[str, Any] = pydantic.Field(default_factory=dict)

    def to_object(self) -> CacheInvalidationErrorPayload:
        return CacheInvalidationErrorPayload(
            error_code=self.error_code,
            error_message=self.error_message,
            error_details=self.error_details,
        )


class CacheInvalidationEntrySchema(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=False)

    status: CacheInvalidationStatus
    payload: dict[str, Any]
    executed_at: float

    def to_object(self) -> CacheInvalidationEntry:
        if self.status == CacheInvalidationStatus.SUCCESS:
            success_payload = CacheInvalidationSuccessPayloadSchema.model_validate(self.payload)
            payload: CacheInvalidationSuccessPayload | CacheInvalidationErrorPayload = success_payload.to_object()
        else:
            error_payload = CacheInvalidationErrorPayloadSchema.model_validate(self.payload)
            payload = error_payload.to_object()

        return CacheInvalidationEntry(
            status=self.status,
            payload=payload,
            executed_at=self.executed_at,
        )


def deserialize_cache_invalidation_entry(data: bytes) -> CacheInvalidationEntry:
    try:
        parsed = json.loads(data.decode("utf-8"))
        schema = CacheInvalidationEntrySchema.model_validate(parsed)
        return schema.to_object()
    except (json.JSONDecodeError, pydantic.ValidationError) as exc:
        raise CacheInvalidationDeserializationError() from exc
