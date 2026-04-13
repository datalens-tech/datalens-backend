import enum
import hashlib
import json
import time
from typing import Any

import attr
import typing_extensions

from dl_constants.exc import (
    DEFAULT_ERR_CODE_API_PREFIX,
    GLOBAL_ERR_PREFIX,
    DLBaseException,
)


KEY_PREFIX = "cache_inval_"


class CacheInvalidationStatus(enum.Enum):
    SUCCESS = "success"
    ERROR = "error"


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class CacheInvalidationKey:
    dataset_id: str
    dataset_revision_id: str
    connection_id: str
    connection_revision_id: str

    def to_redis_key(self) -> str:
        raw = f"{self.dataset_id}:{self.dataset_revision_id}:{self.connection_id}:{self.connection_revision_id}"
        key_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        return f"{KEY_PREFIX}{key_hash}"


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class CacheInvalidationSuccessPayload:
    data: str

    def to_dict(self) -> dict[str, Any]:
        return {"data": self.data}


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class CacheInvalidationErrorPayload:
    error_code: str
    error_message: str | None = None
    error_details: dict[str, Any] = attr.Factory(dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "error_code": self.error_code,
            "error_message": self.error_message,
            "error_details": self.error_details,
        }


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class CacheInvalidationEntry:
    status: CacheInvalidationStatus
    payload: CacheInvalidationSuccessPayload | CacheInvalidationErrorPayload
    executed_at: float  # unix timestamp

    def to_json_bytes(self) -> bytes:
        return json.dumps(
            {
                "status": self.status.value,
                "payload": self.payload.to_dict(),
                "executed_at": self.executed_at,
            },
            ensure_ascii=True,
        ).encode("utf-8")

    @classmethod
    def from_data(cls, data: str) -> typing_extensions.Self:
        return cls(
            status=CacheInvalidationStatus.SUCCESS,
            payload=CacheInvalidationSuccessPayload(data=data),
            executed_at=time.time(),
        )

    @classmethod
    def from_exception(
        cls,
        exc: DLBaseException,
    ) -> typing_extensions.Self:
        error_code = ".".join([GLOBAL_ERR_PREFIX, DEFAULT_ERR_CODE_API_PREFIX] + exc.err_code)
        return cls(
            status=CacheInvalidationStatus.ERROR,
            payload=CacheInvalidationErrorPayload(
                error_code=error_code,
                error_message=exc.message,
                error_details=exc.details,
            ),
            executed_at=time.time(),
        )

    @property
    def is_success(self) -> bool:
        return self.status == CacheInvalidationStatus.SUCCESS

    @property
    def data(self) -> str | None:
        if isinstance(self.payload, CacheInvalidationSuccessPayload):
            return self.payload.data
        return None
