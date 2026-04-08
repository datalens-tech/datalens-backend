"""Unit tests for the invalidation cache engine."""

import json

import pytest

from dl_cache_engine.invalidation import (
    STATUS_ERROR,
    STATUS_SUCCESS,
    InvalidationCacheEntry,
    InvalidationCacheKey,
    InvalidationErrorPayload,
    InvalidationSuccessPayload,
)


# ===== InvalidationCacheKey tests =====


class TestInvalidationCacheKey:
    def test_to_redis_key_format(self) -> None:
        """Key should have the correct prefix and be a sha256 hash."""
        key = InvalidationCacheKey(
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

    def test_same_inputs_produce_same_key(self) -> None:
        """Same inputs should produce the same Redis key."""
        key1 = InvalidationCacheKey(
            dataset_id="ds-1",
            dataset_revision_id="rev-1",
            connection_id="conn-1",
            connection_revision_id="conn-rev-1",
        )
        key2 = InvalidationCacheKey(
            dataset_id="ds-1",
            dataset_revision_id="rev-1",
            connection_id="conn-1",
            connection_revision_id="conn-rev-1",
        )
        assert key1.to_redis_key() == key2.to_redis_key()

    def test_different_inputs_produce_different_keys(self) -> None:
        """Different inputs should produce different Redis keys."""
        key1 = InvalidationCacheKey(
            dataset_id="ds-1",
            dataset_revision_id="rev-1",
            connection_id="conn-1",
            connection_revision_id="conn-rev-1",
        )
        key2 = InvalidationCacheKey(
            dataset_id="ds-1",
            dataset_revision_id="rev-2",  # different revision
            connection_id="conn-1",
            connection_revision_id="conn-rev-1",
        )
        assert key1.to_redis_key() != key2.to_redis_key()

    def test_key_changes_on_connection_revision_change(self) -> None:
        """Key should change when connection revision changes."""
        key1 = InvalidationCacheKey(
            dataset_id="ds-1",
            dataset_revision_id="rev-1",
            connection_id="conn-1",
            connection_revision_id="conn-rev-1",
        )
        key2 = InvalidationCacheKey(
            dataset_id="ds-1",
            dataset_revision_id="rev-1",
            connection_id="conn-1",
            connection_revision_id="conn-rev-2",  # different
        )
        assert key1.to_redis_key() != key2.to_redis_key()

    def test_key_is_frozen(self) -> None:
        """Key should be immutable (frozen attrs)."""
        key = InvalidationCacheKey(
            dataset_id="ds-1",
            dataset_revision_id="rev-1",
            connection_id="conn-1",
            connection_revision_id="conn-rev-1",
        )
        with pytest.raises(AttributeError):
            key.dataset_id = "ds-2"  # type: ignore


# ===== InvalidationSuccessPayload tests =====


class TestInvalidationSuccessPayload:
    def test_to_dict(self) -> None:
        payload = InvalidationSuccessPayload(data="2024-03-27T15:00:00")
        assert payload.to_dict() == {"data": "2024-03-27T15:00:00"}

    def test_from_dict(self) -> None:
        payload = InvalidationSuccessPayload.from_dict({"data": "some_value"})
        assert payload.data == "some_value"

    def test_roundtrip(self) -> None:
        original = InvalidationSuccessPayload(data="test_data")
        restored = InvalidationSuccessPayload.from_dict(original.to_dict())
        assert restored.data == original.data


# ===== InvalidationErrorPayload tests =====


class TestInvalidationErrorPayload:
    def test_to_dict_full(self) -> None:
        payload = InvalidationErrorPayload(
            error_code="ERR.DS.TIMEOUT",
            error_message="Query timeout exceeded: 5s",
            error_details={"timeout_sec": 5},
        )
        d = payload.to_dict()
        assert d["error_code"] == "ERR.DS.TIMEOUT"
        assert d["error_message"] == "Query timeout exceeded: 5s"
        assert d["error_details"] == {"timeout_sec": 5}

    def test_to_dict_minimal(self) -> None:
        payload = InvalidationErrorPayload(error_code="ERR.DS.UNKNOWN")
        d = payload.to_dict()
        assert d["error_code"] == "ERR.DS.UNKNOWN"
        assert d["error_message"] is None
        assert d["error_details"] == {}

    def test_from_dict(self) -> None:
        payload = InvalidationErrorPayload.from_dict(
            {
                "error_code": "ERR.DS.CONN",
                "error_message": "Connection refused",
                "error_details": {"host": "db.example.com"},
            }
        )
        assert payload.error_code == "ERR.DS.CONN"
        assert payload.error_message == "Connection refused"
        assert payload.error_details == {"host": "db.example.com"}

    def test_from_dict_minimal(self) -> None:
        payload = InvalidationErrorPayload.from_dict({"error_code": "ERR.DS.UNKNOWN"})
        assert payload.error_code == "ERR.DS.UNKNOWN"
        assert payload.error_message is None
        assert payload.error_details == {}

    def test_roundtrip(self) -> None:
        original = InvalidationErrorPayload(
            error_code="ERR.DS.TIMEOUT",
            error_message="Timeout",
            error_details={"sec": 5},
        )
        restored = InvalidationErrorPayload.from_dict(original.to_dict())
        assert restored.error_code == original.error_code
        assert restored.error_message == original.error_message
        assert restored.error_details == original.error_details


# ===== InvalidationCacheEntry tests =====


class TestInvalidationCacheEntry:
    def test_make_success(self) -> None:
        entry = InvalidationCacheEntry.make_success(data="2024-03-27T15:00:00")
        assert entry.is_success is True
        assert entry.data == "2024-03-27T15:00:00"
        assert entry.status == STATUS_SUCCESS
        assert isinstance(entry.payload, InvalidationSuccessPayload)
        assert entry.executed_at > 0

    def test_make_error(self) -> None:
        entry = InvalidationCacheEntry.make_error(
            error_code="ERR.DS.TIMEOUT",
            error_message="Query timeout exceeded: 5s",
            error_details={"timeout_sec": 5},
        )
        assert entry.is_success is False
        assert entry.data is None
        assert entry.status == STATUS_ERROR
        assert isinstance(entry.payload, InvalidationErrorPayload)
        assert entry.payload.error_code == "ERR.DS.TIMEOUT"
        assert entry.payload.error_message == "Query timeout exceeded: 5s"
        assert entry.payload.error_details == {"timeout_sec": 5}

    def test_make_error_minimal(self) -> None:
        entry = InvalidationCacheEntry.make_error(error_code="ERR.DS.UNKNOWN")
        assert entry.is_success is False
        assert isinstance(entry.payload, InvalidationErrorPayload)
        assert entry.payload.error_code == "ERR.DS.UNKNOWN"
        assert entry.payload.error_message is None
        assert entry.payload.error_details == {}

    def test_success_json_roundtrip(self) -> None:
        original = InvalidationCacheEntry.make_success(data="test_payload")
        serialized = original.to_json_bytes()
        restored = InvalidationCacheEntry.from_json_bytes(serialized)

        assert restored.status == original.status
        assert restored.data == original.data
        assert restored.executed_at == original.executed_at
        assert isinstance(restored.payload, InvalidationSuccessPayload)

    def test_error_json_roundtrip(self) -> None:
        original = InvalidationCacheEntry.make_error(
            error_code="ERR.DS.TIMEOUT",
            error_message="Timeout",
            error_details={"sec": 5},
        )
        serialized = original.to_json_bytes()
        restored = InvalidationCacheEntry.from_json_bytes(serialized)

        assert restored.status == original.status
        assert restored.is_success is False
        assert isinstance(restored.payload, InvalidationErrorPayload)
        assert restored.payload.error_code == "ERR.DS.TIMEOUT"
        assert restored.payload.error_message == "Timeout"
        assert restored.payload.error_details == {"sec": 5}
        assert restored.executed_at == original.executed_at

    def test_json_format_success(self) -> None:
        """Verify the exact JSON structure for success entries."""
        entry = InvalidationCacheEntry(
            status=STATUS_SUCCESS,
            payload=InvalidationSuccessPayload(data="some_value"),
            executed_at=1642242600.0,
        )
        parsed = json.loads(entry.to_json_bytes().decode("utf-8"))
        assert parsed == {
            "status": "success",
            "payload": {"data": "some_value"},
            "executed_at": 1642242600.0,
        }

    def test_json_format_error(self) -> None:
        """Verify the exact JSON structure for error entries."""
        entry = InvalidationCacheEntry(
            status=STATUS_ERROR,
            payload=InvalidationErrorPayload(
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

    def test_data_property_success(self) -> None:
        entry = InvalidationCacheEntry.make_success(data="payload_value")
        assert entry.data == "payload_value"

    def test_data_property_error(self) -> None:
        entry = InvalidationCacheEntry.make_error(error_code="ERR")
        assert entry.data is None

    def test_from_json_bytes_invalid_json(self) -> None:
        """Should raise on invalid JSON."""
        with pytest.raises(json.JSONDecodeError):
            InvalidationCacheEntry.from_json_bytes(b"not json")

    def test_from_json_bytes_missing_fields(self) -> None:
        """Should raise on missing required fields."""
        with pytest.raises(KeyError):
            InvalidationCacheEntry.from_json_bytes(b'{"status": "success"}')
