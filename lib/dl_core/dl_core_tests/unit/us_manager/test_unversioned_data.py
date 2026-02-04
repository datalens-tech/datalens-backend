from __future__ import annotations

from typing import Any

import attr
from cryptography import fernet
import pytest

from dl_api_commons.base_models import (
    RequestContextInfo,
    TenantCommon,
)
from dl_configs.crypto_keys import CryptoKeysConfig
from dl_core.base_models import BaseAttrsDataModel
from dl_core.services_registry.top_level import (
    DummyServiceRegistry,
    ServicesRegistry,
)
from dl_core.us_entry import USEntry
from dl_core.us_manager.crypto.main import CryptoController
from dl_core.us_manager.us_entry_serializer import (
    USEntrySerializer,
    USEntrySerializerMarshmallow,
    USUnversionedDataPack,
)
from dl_core.us_manager.us_manager import USManagerBase
from dl_core.us_manager.us_manager_sync_mock import MockedSyncUSManager
from dl_utils.utils import DataKey


class TestUSEntry(USEntry):
    @attr.s
    class DataModel(BaseAttrsDataModel):
        secret_field: str | None = attr.ib(default=None)
        unversioned_field: str | None = attr.ib(default=None)

        @classmethod
        def get_secret_keys(cls) -> set[DataKey]:
            return {DataKey(parts=("secret_field",))}

        @classmethod
        def get_unversioned_keys(cls) -> set[DataKey]:
            return {DataKey(parts=("unversioned_field",))}


class TestSerializer(USEntrySerializerMarshmallow):
    def get_secret_keys(self, cls: type[USEntry]) -> set[DataKey]:
        return TestUSEntry.DataModel.get_secret_keys()

    def get_unversioned_keys(self, cls: type[USEntry]) -> set[DataKey]:
        return TestUSEntry.DataModel.get_unversioned_keys()


@pytest.fixture
def crypto_keys_config() -> CryptoKeysConfig:
    return CryptoKeysConfig(
        map_id_key=dict(
            test_key=fernet.Fernet.generate_key().decode("ascii"),
        ),
        actual_key_id="test_key",
    )


@pytest.fixture
def crypto_controller(crypto_keys_config) -> CryptoController:
    return CryptoController(crypto_keys_config)


@pytest.fixture
def bi_context() -> RequestContextInfo:
    return RequestContextInfo.create(
        request_id=None,
        tenant=TenantCommon(),
        user_id=None,
        user_name=None,
        x_dl_debug_mode=False,
        endpoint_code=None,
        x_dl_context=None,
        plain_headers={},
        secret_headers={},
        auth_data=None,
    )


@pytest.fixture
def default_service_registry(bi_context) -> ServicesRegistry:
    return DummyServiceRegistry(rci=bi_context)


@pytest.fixture
def test_us_manager(bi_context, crypto_keys_config, default_service_registry) -> USManagerBase:
    return MockedSyncUSManager(
        bi_context=bi_context,
        crypto_keys_config=crypto_keys_config,
        services_registry=default_service_registry,
    )


@pytest.fixture
def test_serializer() -> USEntrySerializer:
    return TestSerializer()


class TestUnpackUnversionedData:
    def test_unpack_empty_data(self, test_us_manager: USManagerBase, test_serializer: TestSerializer) -> None:
        unversioned_data: dict[str, Any] = {}

        result = test_us_manager.unpack_unversioned_data_from_us(
            cls=TestUSEntry,
            unversioned_data=unversioned_data,
            serializer=test_serializer,
        )

        assert isinstance(result, USUnversionedDataPack)
        assert result.unversioned_data == {}
        assert result.secrets == {}

    def test_unpack_with_secrets(
        self, test_us_manager: USManagerBase, test_serializer: TestSerializer, crypto_controller: CryptoController
    ) -> None:
        unversioned_data = {
            "secret_field": crypto_controller.encrypt_with_actual_key("secret_value"),
        }

        result = test_us_manager.unpack_unversioned_data_from_us(
            cls=TestUSEntry,
            unversioned_data=unversioned_data,
            serializer=test_serializer,
        )

        assert isinstance(result, USUnversionedDataPack)
        assert "secret_field" in result.secrets
        assert result.secrets["secret_field"] == "secret_value"
        assert result.unversioned_data == {}

    def test_unpack_with_unversioned_fields(
        self, test_us_manager: USManagerBase, test_serializer: TestSerializer
    ) -> None:
        unversioned_data = {
            "unversioned_field": "unversioned_value",
        }

        result = test_us_manager.unpack_unversioned_data_from_us(
            cls=TestUSEntry,
            unversioned_data=unversioned_data,
            serializer=test_serializer,
        )

        assert isinstance(result, USUnversionedDataPack)
        assert "unversioned_field" in result.unversioned_data
        assert result.unversioned_data["unversioned_field"] == "unversioned_value"
        assert result.secrets == {}

    def test_unpack_with_mixed_data(
        self, test_us_manager: USManagerBase, test_serializer: TestSerializer, crypto_controller: CryptoController
    ) -> None:
        unversioned_data = {
            "secret_field": crypto_controller.encrypt_with_actual_key("secret_value"),
            "unversioned_field": "unversioned_value",
            "undeclared_field": "undeclared_value",
        }

        result = test_us_manager.unpack_unversioned_data_from_us(
            cls=TestUSEntry,
            unversioned_data=unversioned_data,
            serializer=test_serializer,
        )

        assert isinstance(result, USUnversionedDataPack)
        assert "secret_field" in result.secrets
        assert result.secrets["secret_field"] == "secret_value"
        assert "unversioned_field" in result.unversioned_data
        assert result.unversioned_data["unversioned_field"] == "unversioned_value"
        # Undeclared fields should be ignored
        assert "undeclared_field" not in result.unversioned_data
        assert "undeclared_field" not in result.secrets


class TestPackUnversionedData:
    def test_pack_empty_data(self, test_us_manager: USManagerBase, test_serializer: TestSerializer) -> None:
        data_pack = USUnversionedDataPack()

        result = test_us_manager.pack_unversioned_data_for_us(
            cls=TestUSEntry,
            data_pack=data_pack,
            serializer=test_serializer,
        )

        assert isinstance(result, dict)
        assert result == {}

    def test_pack_with_secrets(
        self, test_us_manager: USManagerBase, test_serializer: TestSerializer, crypto_controller: CryptoController
    ) -> None:
        data_pack = USUnversionedDataPack(
            secrets={
                "secret_field": "secret_value",
            }
        )

        result = test_us_manager.pack_unversioned_data_for_us(
            cls=TestUSEntry,
            data_pack=data_pack,
            serializer=test_serializer,
        )

        assert isinstance(result, dict)
        assert "secret_field" in result

        # Verify that values are encrypted
        assert isinstance(result["secret_field"], dict)
        assert "key_id" in result["secret_field"]
        assert "key_kind" in result["secret_field"]
        assert "cypher_text" in result["secret_field"]

        # Verify that decrypting gives original values
        assert crypto_controller.decrypt(result["secret_field"]) == "secret_value"

    def test_pack_with_unversioned_fields(
        self, test_us_manager: USManagerBase, test_serializer: TestSerializer
    ) -> None:
        data_pack = USUnversionedDataPack(
            unversioned_data={
                "unversioned_field": "unversioned_value",
            }
        )

        result = test_us_manager.pack_unversioned_data_for_us(
            cls=TestUSEntry,
            data_pack=data_pack,
            serializer=test_serializer,
        )

        assert isinstance(result, dict)
        assert "unversioned_field" in result
        assert result["unversioned_field"] == "unversioned_value"

    def test_pack_with_mixed_data(
        self, test_us_manager: USManagerBase, test_serializer: TestSerializer, crypto_controller: CryptoController
    ) -> None:
        data_pack = USUnversionedDataPack(
            secrets={
                "secret_field": "secret_value",
            },
            unversioned_data={
                "unversioned_field": "unversioned_value",
            },
        )

        result = test_us_manager.pack_unversioned_data_for_us(
            cls=TestUSEntry,
            data_pack=data_pack,
            serializer=test_serializer,
        )

        assert isinstance(result, dict)
        assert "secret_field" in result
        assert "unversioned_field" in result

        # Verify that password is encrypted
        assert isinstance(result["secret_field"], dict)
        assert "key_id" in result["secret_field"]
        assert "key_kind" in result["secret_field"]
        assert "cypher_text" in result["secret_field"]

        # Verify that unversioned field is not encrypted
        assert result["unversioned_field"] == "unversioned_value"

        # Verify that decrypting gives original values
        assert crypto_controller.decrypt(result["secret_field"]) == "secret_value"

    def test_pack_with_none_secret(
        self, test_us_manager: USManagerBase, test_serializer: TestSerializer, crypto_controller: CryptoController
    ) -> None:
        data_pack = USUnversionedDataPack(
            secrets={
                "secret_field": None,
            }
        )

        result = test_us_manager.pack_unversioned_data_for_us(
            cls=TestUSEntry,
            data_pack=data_pack,
            serializer=test_serializer,
        )

        assert isinstance(result, dict)
        assert "secret_field" in result

        # Verify that None is encrypted as None
        assert crypto_controller.decrypt(result["secret_field"]) is None


class TestRoundTrip:
    def test_round_trip_with_secrets(
        self, test_us_manager: USManagerBase, test_serializer: TestSerializer, crypto_controller: CryptoController
    ) -> None:
        original_unversioned_data = {
            "secret_field": crypto_controller.encrypt_with_actual_key("secret_value"),
        }

        # Unpack
        data_pack = test_us_manager.unpack_unversioned_data_from_us(
            cls=TestUSEntry,
            unversioned_data=original_unversioned_data,
            serializer=test_serializer,
        )

        # Pack
        result = test_us_manager.pack_unversioned_data_for_us(
            cls=TestUSEntry,
            data_pack=data_pack,
            serializer=test_serializer,
        )

        assert isinstance(result, dict)
        assert "secret_field" in result

        # Values should be re-encrypted, so they won't be identical to the original
        # But decrypting should give the same values
        assert crypto_controller.decrypt(result["secret_field"]) == "secret_value"

    def test_round_trip_with_mixed_data(
        self, test_us_manager: USManagerBase, test_serializer: TestSerializer, crypto_controller: CryptoController
    ) -> None:
        original_unversioned_data = {
            "secret_field": crypto_controller.encrypt_with_actual_key("secret_value"),
            "unversioned_field": "unversioned_value",
        }

        # Unpack
        data_pack = test_us_manager.unpack_unversioned_data_from_us(
            cls=TestUSEntry,
            unversioned_data=original_unversioned_data,
            serializer=test_serializer,
        )

        # Pack
        result = test_us_manager.pack_unversioned_data_for_us(
            cls=TestUSEntry,
            data_pack=data_pack,
            serializer=test_serializer,
        )

        assert isinstance(result, dict)
        assert "secret_field" in result
        assert "unversioned_field" in result

        # Password should be re-encrypted, unversioned field should be unchanged
        assert crypto_controller.decrypt(result["secret_field"]) == "secret_value"
        assert result["unversioned_field"] == "unversioned_value"
