import json

import pydantic
import pytest

from dl_cache_engine.cache_invalidation import exc as inval_exc
from dl_cache_engine.cache_invalidation.exc import CacheInvalidationDeserializationError
from dl_cache_engine.cache_invalidation.primitives import (
    CacheInvalidationEntry,
    CacheInvalidationErrorPayload,
    CacheInvalidationKey,
    CacheInvalidationStatus,
    CacheInvalidationSuccessPayload,
)
from dl_cache_engine.cache_invalidation.schemas import (
    CacheInvalidationEntrySchema,
    CacheInvalidationErrorPayloadSchema,
    CacheInvalidationSuccessPayloadSchema,
    deserialize_cache_invalidation_entry,
)


# ===== CacheInvalidationKey tests =====


def test_key_to_redis_key_format() -> None:
    key = CacheInvalidationKey(
        dataset_id="ds-1",
        dataset_revision_id="rev-1",
        connection_id="conn-1",
        connection_revision_id="conn-rev-1",
    )
    redis_key = key.to_redis_key()
    assert redis_key.startswith("cache_inval_")
    # sha256 hex digest is 64 chars
    hash_part = redis_key[len("cache_inval_") :]
    assert len(hash_part) == 64


def test_key_same_inputs_produce_same_key() -> None:
    key1 = CacheInvalidationKey(
        dataset_id="ds-1",
        dataset_revision_id="rev-1",
        connection_id="conn-1",
        connection_revision_id="conn-rev-1",
    )
    key2 = CacheInvalidationKey(
        dataset_id="ds-1",
        dataset_revision_id="rev-1",
        connection_id="conn-1",
        connection_revision_id="conn-rev-1",
    )
    assert key1.to_redis_key() == key2.to_redis_key()


def test_key_different_inputs_produce_different_keys() -> None:
    key1 = CacheInvalidationKey(
        dataset_id="ds-1",
        dataset_revision_id="rev-1",
        connection_id="conn-1",
        connection_revision_id="conn-rev-1",
    )
    key2 = CacheInvalidationKey(
        dataset_id="ds-1",
        dataset_revision_id="rev-2",  # different revision
        connection_id="conn-1",
        connection_revision_id="conn-rev-1",
    )
    assert key1.to_redis_key() != key2.to_redis_key()


def test_key_changes_on_connection_revision_change() -> None:
    key1 = CacheInvalidationKey(
        dataset_id="ds-1",
        dataset_revision_id="rev-1",
        connection_id="conn-1",
        connection_revision_id="conn-rev-1",
    )
    key2 = CacheInvalidationKey(
        dataset_id="ds-1",
        dataset_revision_id="rev-1",
        connection_id="conn-1",
        connection_revision_id="conn-rev-2",  # different
    )
    assert key1.to_redis_key() != key2.to_redis_key()


def test_key_is_frozen() -> None:
    key = CacheInvalidationKey(
        dataset_id="ds-1",
        dataset_revision_id="rev-1",
        connection_id="conn-1",
        connection_revision_id="conn-rev-1",
    )
    with pytest.raises(AttributeError):
        key.dataset_id = "ds-2"  # type: ignore


# ===== CacheInvalidationSuccessPayload tests =====


def test_success_payload_to_dict() -> None:
    payload = CacheInvalidationSuccessPayload(data="2024-03-27T15:00:00")
    assert payload.to_dict() == {"data": "2024-03-27T15:00:00"}


def test_success_payload_schema_load() -> None:
    schema = CacheInvalidationSuccessPayloadSchema.model_validate({"data": "some_value"})
    payload = schema.to_object()
    assert isinstance(payload, CacheInvalidationSuccessPayload)
    assert payload.data == "some_value"


def test_success_payload_schema_load_missing_data() -> None:
    with pytest.raises(pydantic.ValidationError):
        CacheInvalidationSuccessPayloadSchema.model_validate({})


def test_success_payload_roundtrip() -> None:
    original = CacheInvalidationSuccessPayload(data="test_data")
    schema = CacheInvalidationSuccessPayloadSchema.model_validate(original.to_dict())
    restored = schema.to_object()
    assert restored.data == original.data


# ===== CacheInvalidationErrorPayload tests =====


def test_error_payload_to_dict_full() -> None:
    payload = CacheInvalidationErrorPayload(
        error_code="ERR.DS.TIMEOUT",
        error_message="Query timeout exceeded: 5s",
        error_details={"timeout_sec": 5},
    )
    d = payload.to_dict()
    assert d["error_code"] == "ERR.DS.TIMEOUT"
    assert d["error_message"] == "Query timeout exceeded: 5s"
    assert d["error_details"] == {"timeout_sec": 5}


def test_error_payload_to_dict_minimal() -> None:
    payload = CacheInvalidationErrorPayload(error_code="ERR.DS.UNKNOWN")
    d = payload.to_dict()
    assert d["error_code"] == "ERR.DS.UNKNOWN"
    assert d["error_message"] is None
    assert d["error_details"] == {}


def test_error_payload_schema_load() -> None:
    schema = CacheInvalidationErrorPayloadSchema.model_validate(
        {
            "error_code": "ERR.DS.CONN",
            "error_message": "Connection refused",
            "error_details": {"host": "db.example.com"},
        }
    )
    payload = schema.to_object()
    assert isinstance(payload, CacheInvalidationErrorPayload)
    assert payload.error_code == "ERR.DS.CONN"
    assert payload.error_message == "Connection refused"
    assert payload.error_details == {"host": "db.example.com"}


def test_error_payload_schema_load_minimal() -> None:
    schema = CacheInvalidationErrorPayloadSchema.model_validate({"error_code": "ERR.DS.UNKNOWN"})
    payload = schema.to_object()
    assert isinstance(payload, CacheInvalidationErrorPayload)
    assert payload.error_code == "ERR.DS.UNKNOWN"
    assert payload.error_message is None
    assert payload.error_details == {}


def test_error_payload_schema_load_missing_error_code() -> None:
    with pytest.raises(pydantic.ValidationError):
        CacheInvalidationErrorPayloadSchema.model_validate({})


def test_error_payload_roundtrip() -> None:
    original = CacheInvalidationErrorPayload(
        error_code="ERR.DS.TIMEOUT",
        error_message="Timeout",
        error_details={"sec": 5},
    )
    schema = CacheInvalidationErrorPayloadSchema.model_validate(original.to_dict())
    restored = schema.to_object()
    assert restored.error_code == original.error_code
    assert restored.error_message == original.error_message
    assert restored.error_details == original.error_details


# ===== CacheInvalidationEntry tests =====


def test_entry_make_success() -> None:
    entry = CacheInvalidationEntry.from_data(data="2024-03-27T15:00:00")
    assert entry.is_success is True
    assert entry.data == "2024-03-27T15:00:00"
    assert entry.status == CacheInvalidationStatus.SUCCESS
    assert isinstance(entry.payload, CacheInvalidationSuccessPayload)
    assert entry.executed_at > 0


def test_entry_make_error() -> None:
    entry = CacheInvalidationEntry.from_exception(
        inval_exc.CacheInvalidationQueryTimeoutError(
            message="Query timeout exceeded: 5s",
            details={"timeout_sec": 5},
        )
    )
    assert entry.is_success is False
    assert entry.data is None
    assert entry.status == CacheInvalidationStatus.ERROR
    assert isinstance(entry.payload, CacheInvalidationErrorPayload)
    assert entry.payload.error_code == "ERR.DS_API.CACHE_INVALIDATION.QUERY_TIMEOUT"
    assert entry.payload.error_message == "Query timeout exceeded: 5s"
    assert entry.payload.error_details == {"timeout_sec": 5}


def test_entry_make_error_minimal() -> None:
    entry = CacheInvalidationEntry.from_exception(inval_exc.CacheInvalidationQueryError())
    assert entry.is_success is False
    assert isinstance(entry.payload, CacheInvalidationErrorPayload)
    assert entry.payload.error_code == "ERR.DS_API.CACHE_INVALIDATION.QUERY_ERROR"
    assert entry.payload.error_message == "Cache invalidation query execution failed"
    assert entry.payload.error_details == {}


def test_entry_success_json_roundtrip() -> None:
    original = CacheInvalidationEntry.from_data(data="test_payload")
    serialized = original.to_json_bytes()
    restored = deserialize_cache_invalidation_entry(serialized)

    assert restored.status == original.status
    assert restored.data == original.data
    assert restored.executed_at == original.executed_at
    assert isinstance(restored.payload, CacheInvalidationSuccessPayload)


def test_entry_error_json_roundtrip() -> None:
    original = CacheInvalidationEntry.from_exception(
        inval_exc.CacheInvalidationQueryTimeoutError(
            message="Timeout",
            details={"sec": 5},
        )
    )
    serialized = original.to_json_bytes()
    restored = deserialize_cache_invalidation_entry(serialized)

    assert restored.status == original.status
    assert restored.is_success is False
    assert isinstance(restored.payload, CacheInvalidationErrorPayload)
    assert restored.payload.error_code == "ERR.DS_API.CACHE_INVALIDATION.QUERY_TIMEOUT"
    assert restored.payload.error_message == "Timeout"
    assert restored.payload.error_details == {"sec": 5}
    assert restored.executed_at == original.executed_at


def test_entry_json_format_success() -> None:
    entry = CacheInvalidationEntry(
        status=CacheInvalidationStatus.SUCCESS,
        payload=CacheInvalidationSuccessPayload(data="some_value"),
        executed_at=1642242600.0,
    )
    parsed = json.loads(entry.to_json_bytes().decode("utf-8"))
    assert parsed == {
        "status": "success",
        "payload": {"data": "some_value"},
        "executed_at": 1642242600.0,
    }


def test_entry_json_format_error() -> None:
    entry = CacheInvalidationEntry(
        status=CacheInvalidationStatus.ERROR,
        payload=CacheInvalidationErrorPayload(
            error_code="ERR.DS.TIMEOUT",
            error_message="Query timeout exceeded: 5s",
            error_details={"timeout_sec": 5},
        ),
        executed_at=1642242600.0,
    )
    parsed = json.loads(entry.to_json_bytes().decode("utf-8"))
    assert parsed == {
        "status": "error",
        "payload": {
            "error_code": "ERR.DS.TIMEOUT",
            "error_message": "Query timeout exceeded: 5s",
            "error_details": {"timeout_sec": 5},
        },
        "executed_at": 1642242600.0,
    }


def test_entry_data_property_success() -> None:
    entry = CacheInvalidationEntry.from_data(data="payload_value")
    assert entry.data == "payload_value"


def test_entry_data_property_error() -> None:
    entry = CacheInvalidationEntry.from_exception(inval_exc.CacheInvalidationBaseError())
    assert entry.data is None


def test_deserialize_invalid_json() -> None:
    with pytest.raises(CacheInvalidationDeserializationError):
        deserialize_cache_invalidation_entry(b"not json")


def test_deserialize_invalid_json_preserves_cause() -> None:
    with pytest.raises(CacheInvalidationDeserializationError) as exc_info:
        deserialize_cache_invalidation_entry(b"not json")
    assert isinstance(exc_info.value.__cause__, json.JSONDecodeError)


def test_deserialize_missing_fields() -> None:
    with pytest.raises(CacheInvalidationDeserializationError):
        deserialize_cache_invalidation_entry(b'{"status": "success"}')


def test_deserialize_invalid_status() -> None:
    with pytest.raises(CacheInvalidationDeserializationError):
        deserialize_cache_invalidation_entry(b'{"status": "unknown", "payload": {}, "executed_at": 1.0}')


def test_deserialize_missing_payload_fields() -> None:
    with pytest.raises(CacheInvalidationDeserializationError):
        deserialize_cache_invalidation_entry(b'{"status": "success", "payload": {}, "executed_at": 1.0}')


def test_entry_schema_load_success() -> None:
    schema = CacheInvalidationEntrySchema.model_validate(
        {
            "status": "success",
            "payload": {"data": "test_value"},
            "executed_at": 1642242600.0,
        }
    )
    entry = schema.to_object()
    assert isinstance(entry, CacheInvalidationEntry)
    assert entry.status == CacheInvalidationStatus.SUCCESS
    assert isinstance(entry.payload, CacheInvalidationSuccessPayload)
    assert entry.payload.data == "test_value"
    assert entry.executed_at == 1642242600.0


def test_entry_schema_load_error() -> None:
    schema = CacheInvalidationEntrySchema.model_validate(
        {
            "status": "error",
            "payload": {
                "error_code": "ERR.DS.TIMEOUT",
                "error_message": "Timeout",
                "error_details": {"sec": 5},
            },
            "executed_at": 1642242600.0,
        }
    )
    entry = schema.to_object()
    assert isinstance(entry, CacheInvalidationEntry)
    assert entry.status == CacheInvalidationStatus.ERROR
    assert isinstance(entry.payload, CacheInvalidationErrorPayload)
    assert entry.payload.error_code == "ERR.DS.TIMEOUT"
