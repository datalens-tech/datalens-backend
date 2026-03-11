from __future__ import annotations

from collections import ChainMap
from typing import Any
from typing import ChainMap as ChainMapGeneric

import attr
from cryptography import fernet
import marshmallow
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
from dl_core.us_manager.storage_schemas.base import DefaultStorageSchema
from dl_core.us_manager.us_entry_serializer import (
    USEntrySerializer,
    USEntrySerializerMarshmallow,
    USUnversionedDataPack,
)
from dl_core.us_manager.us_manager import USManagerBase
from dl_core.us_manager.us_manager_sync_mock import MockedSyncUSManager
from dl_utils.utils import DataKey


@attr.s
class CatShapeProperties(BaseAttrsDataModel):
    radius: int | None = attr.ib(default=None)
    mass: float | None = attr.ib(default=None)
    is_spherical: bool | None = attr.ib(default=False)
    fluidity_coefficient: float | None = attr.ib(default=False)
    signature: str | None = attr.ib(default=None)


@attr.s
class CatProperties(BaseAttrsDataModel):
    shape: CatShapeProperties | None = attr.ib(default=None)
    color: str | None = attr.ib(default=None)
    name: str | None = attr.ib(default=None)
    last_fed: int | None = attr.ib(default=None)


class TestUSEntry(USEntry):
    @attr.s
    class DataModel(BaseAttrsDataModel):
        secret_field: str | None = attr.ib(default=None)
        unversioned_field: str | None = attr.ib(default=None)

        cat_properties: CatProperties | None = attr.ib(default=None)

        @classmethod
        def get_secret_keys(cls) -> set[DataKey]:
            return {
                DataKey(parts=("secret_field",)),
                DataKey(parts=("cat_properties", "shape", "signature")),
            }

        @classmethod
        def get_unversioned_keys(cls) -> set[DataKey]:
            return {
                DataKey(parts=("unversioned_field",)),
                DataKey(
                    parts=(
                        "cat_properties",
                        "last_fed",
                    )
                ),
                DataKey(
                    parts=(
                        "cat_properties",
                        "shape",
                        "mass",
                    )
                ),
                DataKey(
                    parts=(
                        "cat_properties",
                        "shape",
                        "radius",
                    )
                ),
            }


class CatShapePropertiesStorageSchema(DefaultStorageSchema):
    TARGET_CLS = CatShapeProperties

    radius = marshmallow.fields.Integer(allow_none=True)
    mass = marshmallow.fields.Float(allow_none=True)
    is_spherical = marshmallow.fields.Boolean(allow_none=True)
    fluidity_coefficient = marshmallow.fields.Float(allow_none=True)
    signature = marshmallow.fields.String(allow_none=True)


class CatPropertiesStorageSchema(DefaultStorageSchema):
    TARGET_CLS = CatProperties

    shape = marshmallow.fields.Nested(CatShapePropertiesStorageSchema, allow_none=True)
    color = marshmallow.fields.String(allow_none=True)
    name = marshmallow.fields.String(allow_none=True)
    last_fed = marshmallow.fields.Integer(allow_none=True)


class TestUSEntryDataModelStorageSchema(DefaultStorageSchema):
    TARGET_CLS = TestUSEntry.DataModel

    secret_field = marshmallow.fields.String(allow_none=True)
    unversioned_field = marshmallow.fields.String(allow_none=True)
    cat_properties = marshmallow.fields.Nested(CatPropertiesStorageSchema, allow_none=True)


class TestSerializer(USEntrySerializerMarshmallow):
    _MAP_TYPE_TO_SCHEMA: ChainMapGeneric[type[BaseAttrsDataModel], type[marshmallow.Schema]] = ChainMap(
        USEntrySerializerMarshmallow._MAP_TYPE_TO_SCHEMA,
        {
            TestUSEntry.DataModel: TestUSEntryDataModelStorageSchema,
            CatProperties: CatPropertiesStorageSchema,
            CatShapeProperties: CatShapePropertiesStorageSchema,
        },
    )

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

    def test_unpack_with_complete_cat_properties(
        self,
        test_us_manager: USManagerBase,
        test_serializer: TestSerializer,
        crypto_controller: CryptoController,
    ) -> None:
        # Include all fields, but only subset of them is really used as defined by get_unversioned_fields
        unversioned_data = {
            "cat_properties": {
                "last_fed": 1234567890,
                "color": "orange",
                "name": "Fluffy",
                "shape": {
                    "mass": 5.5,
                    "radius": 10,
                    "is_spherical": True,
                    "fluidity_coefficient": 0.8,
                    "signature": crypto_controller.encrypt_with_actual_key("red paws"),
                },
            }
        }

        result = test_us_manager.unpack_unversioned_data_from_us(
            cls=TestUSEntry,
            unversioned_data=unversioned_data,
            serializer=test_serializer,
        )

        assert isinstance(result, USUnversionedDataPack)
        # Only unversioned fields should be extracted
        assert "cat_properties" in result.unversioned_data
        assert result.unversioned_data["cat_properties"]["last_fed"] == 1234567890
        assert result.unversioned_data["cat_properties"]["shape"]["mass"] == 5.5
        assert result.unversioned_data["cat_properties"]["shape"]["radius"] == 10
        # Versioned fields should not be in unversioned_data
        assert "color" not in result.unversioned_data.get("cat_properties", {})
        assert "name" not in result.unversioned_data.get("cat_properties", {})
        assert "is_spherical" not in result.unversioned_data.get("cat_properties", {}).get("shape", {})
        assert "fluidity_coefficient" not in result.unversioned_data.get("cat_properties", {}).get("shape", {})
        # The signature field should be in secrets, not unversioned_data
        assert "cat_properties" in result.secrets
        assert result.secrets["cat_properties"]["shape"]["signature"] == "red paws"

    def test_unpack_with_cat_properties_none_values(
        self,
        test_us_manager: USManagerBase,
        test_serializer: TestSerializer,
    ) -> None:
        unversioned_data = {
            "cat_properties": {
                "last_fed": None,
                "shape": {
                    "mass": None,
                    "radius": 15,
                    "signature": None,
                },
            }
        }

        result = test_us_manager.unpack_unversioned_data_from_us(
            cls=TestUSEntry,
            unversioned_data=unversioned_data,
            serializer=test_serializer,
        )

        assert isinstance(result, USUnversionedDataPack)
        assert "cat_properties" in result.unversioned_data
        assert result.unversioned_data["cat_properties"]["last_fed"] is None
        assert result.unversioned_data["cat_properties"]["shape"]["mass"] is None
        assert result.unversioned_data["cat_properties"]["shape"]["radius"] == 15
        # The signature field should be in secrets (even if None)
        assert "cat_properties" in result.secrets
        assert result.secrets["cat_properties"]["shape"]["signature"] is None


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

    def test_pack_with_cat_properties_unversioned_fields(
        self,
        test_us_manager: USManagerBase,
        test_serializer: TestSerializer,
        crypto_controller: CryptoController,
    ) -> None:
        data_pack = USUnversionedDataPack(
            unversioned_data={
                "cat_properties": {
                    "last_fed": 1234567890,
                    "shape": {
                        "mass": 5.5,
                        "radius": 10,
                    },
                }
            },
            secrets={
                "cat_properties": {
                    "shape": {
                        "signature": "red paws",
                    },
                }
            },
        )

        result = test_us_manager.pack_unversioned_data_for_us(
            cls=TestUSEntry,
            data_pack=data_pack,
            serializer=test_serializer,
        )

        assert isinstance(result, dict)
        assert "cat_properties" in result
        assert result["cat_properties"]["last_fed"] == 1234567890
        assert result["cat_properties"]["shape"]["mass"] == 5.5
        assert result["cat_properties"]["shape"]["radius"] == 10
        assert crypto_controller.decrypt(result["cat_properties"]["shape"]["signature"]) == "red paws"

    def test_pack_with_cat_properties_none_values(
        self,
        test_us_manager: USManagerBase,
        test_serializer: TestSerializer,
    ) -> None:
        data_pack = USUnversionedDataPack(
            unversioned_data={
                "cat_properties": {
                    "last_fed": None,
                    "shape": {
                        "mass": None,
                        "radius": 15,
                    },
                }
            },
            secrets={
                "cat_properties": {
                    "shape": {
                        "signature": None,
                    },
                }
            },
        )

        result = test_us_manager.pack_unversioned_data_for_us(
            cls=TestUSEntry,
            data_pack=data_pack,
            serializer=test_serializer,
        )

        assert isinstance(result, dict)
        assert "cat_properties" in result
        assert result["cat_properties"]["last_fed"] is None
        assert result["cat_properties"]["shape"]["mass"] is None
        assert result["cat_properties"]["shape"]["radius"] == 15
        assert result["cat_properties"]["shape"]["signature"] is None


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

    def test_round_trip_with_cat_properties(
        self,
        test_us_manager: USManagerBase,
        test_serializer: TestSerializer,
        crypto_controller: CryptoController,
    ) -> None:
        original_unversioned_data = {
            "cat_properties": {
                "last_fed": 1234567890,
                "shape": {
                    "mass": 5.5,
                    "radius": 10,
                    "signature": crypto_controller.encrypt_with_actual_key("red paws"),
                },
            }
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
        assert "cat_properties" in result
        assert result["cat_properties"]["last_fed"] == 1234567890
        assert result["cat_properties"]["shape"]["mass"] == 5.5
        assert result["cat_properties"]["shape"]["radius"] == 10

        # The signature field should be in secrets (even if None)
        assert crypto_controller.decrypt(result["cat_properties"]["shape"]["signature"]) == "red paws"

    def test_round_trip_with_cat_properties_none_values(
        self,
        test_us_manager: USManagerBase,
        test_serializer: TestSerializer,
    ) -> None:
        original_unversioned_data = {
            "cat_properties": {
                "last_fed": None,
                "shape": {
                    "mass": None,
                    "radius": 15,
                    "signature": None,
                },
            }
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
        assert "cat_properties" in result
        assert result["cat_properties"]["last_fed"] is None
        assert result["cat_properties"]["shape"]["mass"] is None
        assert result["cat_properties"]["shape"]["radius"] == 15

        # The signature field should be in secrets (even if None)
        assert result["cat_properties"]["shape"]["signature"] is None


class TestSerializationDeserialization:
    """
    Test full serialization/deserialization of objects with both versioned and unversioned fields
    """

    def test_serialize_deserialize_complete(
        self,
        test_us_manager: USManagerBase,
        test_serializer: TestSerializer,
    ) -> None:
        # Create a test entry with complete cat_properties data
        entry = TestUSEntry(
            uuid="test-id",
            us_manager=test_us_manager,
            data=TestUSEntry.DataModel(
                secret_field="secret_value",
                unversioned_field="unversioned_value",
                cat_properties=CatProperties(
                    last_fed=1234567890,  # unversioned
                    color="orange",  # versioned
                    name="Fluffy",  # versioned
                    shape=CatShapeProperties(  # mixed
                        mass=5.5,  # unversioned
                        radius=10,  # unversioned
                        is_spherical=True,  # versioned
                        fluidity_coefficient=0.8,  # versioned
                        signature="red paws",  # secret
                    ),
                ),
            ),
            data_strict=False,
        )

        # Serialize
        data_pack = test_serializer.serialize(entry)

        # Verify that unversioned fields are separated
        assert "cat_properties" in data_pack.unversioned_data.unversioned_data
        assert data_pack.unversioned_data.unversioned_data["cat_properties"]["last_fed"] == 1234567890
        assert data_pack.unversioned_data.unversioned_data["cat_properties"]["shape"]["mass"] == 5.5
        assert data_pack.unversioned_data.unversioned_data["cat_properties"]["shape"]["radius"] == 10

        # Verify that secrets are separated
        assert "secret_field" in data_pack.unversioned_data.secrets
        assert data_pack.unversioned_data.secrets["secret_field"] == "secret_value"
        assert "cat_properties" in data_pack.unversioned_data.secrets
        assert data_pack.unversioned_data.secrets["cat_properties"]["shape"]["signature"] == "red paws"

        # Verify that versioned data remains in main data
        assert "cat_properties" in data_pack.data
        assert data_pack.data["cat_properties"]["color"] == "orange"
        assert data_pack.data["cat_properties"]["name"] == "Fluffy"
        assert data_pack.data["cat_properties"]["shape"]["is_spherical"] is True
        assert data_pack.data["cat_properties"]["shape"]["fluidity_coefficient"] == 0.8

        # Deserialize
        deserialized_entry = test_serializer.deserialize(
            cls=TestUSEntry,
            data_pack=data_pack,
            entry_id="test-id",
            us_manager=test_us_manager,
            common_properties={},
            data_strict=False,
        )

        # Verify that the deserialized entry has all the original data
        assert deserialized_entry.data.secret_field == "secret_value"
        assert deserialized_entry.data.unversioned_field == "unversioned_value"
        assert deserialized_entry.data.cat_properties is not None
        assert deserialized_entry.data.cat_properties.last_fed == 1234567890
        assert deserialized_entry.data.cat_properties.color == "orange"
        assert deserialized_entry.data.cat_properties.name == "Fluffy"
        assert deserialized_entry.data.cat_properties.shape is not None
        assert deserialized_entry.data.cat_properties.shape.mass == 5.5
        assert deserialized_entry.data.cat_properties.shape.radius == 10
        assert deserialized_entry.data.cat_properties.shape.is_spherical is True
        assert deserialized_entry.data.cat_properties.shape.fluidity_coefficient == 0.8
        assert deserialized_entry.data.cat_properties.shape.signature == "red paws"

    def test_serialize_deserialize_none_values(
        self,
        test_us_manager: USManagerBase,
        test_serializer: TestSerializer,
    ) -> None:
        # Create a test entry with None values in unversioned fields
        entry = TestUSEntry(
            uuid="test-id",
            us_manager=test_us_manager,
            data=TestUSEntry.DataModel(
                secret_field=None,
                unversioned_field=None,
                cat_properties=CatProperties(
                    last_fed=None,  # unversioned None
                    color="white",  # versioned
                    name=None,  # versioned None
                    shape=CatShapeProperties(  # mixed
                        mass=None,  # unversioned None
                        radius=15,  # unversioned with value
                        is_spherical=False,  # versioned
                        fluidity_coefficient=None,  # versioned None
                        signature=None,  # secret None
                    ),
                ),
            ),
            data_strict=False,
        )

        # Serialize
        data_pack = test_serializer.serialize(entry)

        # Verify that None values are handled correctly in unversioned data
        assert "cat_properties" in data_pack.unversioned_data.unversioned_data
        assert data_pack.unversioned_data.unversioned_data["cat_properties"]["last_fed"] is None
        assert data_pack.unversioned_data.unversioned_data["cat_properties"]["shape"]["mass"] is None
        assert data_pack.unversioned_data.unversioned_data["cat_properties"]["shape"]["radius"] == 15

        # Verify that None secrets are handled correctly
        assert "secret_field" in data_pack.unversioned_data.secrets
        assert data_pack.unversioned_data.secrets["secret_field"] is None
        assert "cat_properties" in data_pack.unversioned_data.secrets
        assert data_pack.unversioned_data.secrets["cat_properties"]["shape"]["signature"] is None

        # Deserialize
        deserialized_entry = test_serializer.deserialize(
            cls=TestUSEntry,
            data_pack=data_pack,
            entry_id="test-id",
            us_manager=test_us_manager,
            common_properties={},
            data_strict=False,
        )

        # Verify that None values are preserved
        assert deserialized_entry.data.secret_field is None
        assert deserialized_entry.data.unversioned_field is None
        assert deserialized_entry.data.cat_properties is not None
        assert deserialized_entry.data.cat_properties.last_fed is None
        assert deserialized_entry.data.cat_properties.color == "white"
        assert deserialized_entry.data.cat_properties.name is None
        assert deserialized_entry.data.cat_properties.shape is not None
        assert deserialized_entry.data.cat_properties.shape.mass is None
        assert deserialized_entry.data.cat_properties.shape.radius == 15
        assert deserialized_entry.data.cat_properties.shape.is_spherical is False
        assert deserialized_entry.data.cat_properties.shape.fluidity_coefficient is None
        assert deserialized_entry.data.cat_properties.shape.signature is None

    def test_serialize_deserialize_only_versioned(
        self,
        test_us_manager: USManagerBase,
        test_serializer: TestSerializer,
    ) -> None:
        # Create a test entry with only versioned cat_properties fields
        entry = TestUSEntry(
            uuid="test-id",
            us_manager=test_us_manager,
            data=TestUSEntry.DataModel(
                secret_field="secret_value",
                unversioned_field="unversioned_value",
                cat_properties=CatProperties(
                    last_fed=None,  # unversioned but None
                    color="purple",  # versioned
                    name="Violet",  # versioned
                    shape=CatShapeProperties(
                        mass=None,  # unversioned but None
                        radius=None,  # unversioned but None
                        is_spherical=True,  # versioned
                        fluidity_coefficient=0.9,  # versioned
                        signature=None,  # secret
                    ),
                ),
            ),
            data_strict=False,
        )

        # Serialize
        data_pack = test_serializer.serialize(entry)

        # Verify that None unversioned fields are still included
        assert "cat_properties" in data_pack.unversioned_data.unversioned_data
        assert data_pack.unversioned_data.unversioned_data["cat_properties"]["last_fed"] is None
        assert data_pack.unversioned_data.unversioned_data["cat_properties"]["shape"]["mass"] is None
        assert data_pack.unversioned_data.unversioned_data["cat_properties"]["shape"]["radius"] is None

        # Verify that versioned data remains in main data
        assert "cat_properties" in data_pack.data
        assert data_pack.data["cat_properties"]["color"] == "purple"
        assert data_pack.data["cat_properties"]["name"] == "Violet"
        assert data_pack.data["cat_properties"]["shape"]["is_spherical"] is True
        assert data_pack.data["cat_properties"]["shape"]["fluidity_coefficient"] == 0.9

        # Deserialize
        deserialized_entry = test_serializer.deserialize(
            cls=TestUSEntry,
            data_pack=data_pack,
            entry_id="test-id",
            us_manager=test_us_manager,
            common_properties={},
            data_strict=False,
        )

        # Verify that all data is correctly reconstructed
        assert deserialized_entry.data.cat_properties is not None
        assert deserialized_entry.data.cat_properties.last_fed is None
        assert deserialized_entry.data.cat_properties.color == "purple"
        assert deserialized_entry.data.cat_properties.name == "Violet"
        assert deserialized_entry.data.cat_properties.shape is not None
        assert deserialized_entry.data.cat_properties.shape.mass is None
        assert deserialized_entry.data.cat_properties.shape.radius is None
        assert deserialized_entry.data.cat_properties.shape.is_spherical is True
        assert deserialized_entry.data.cat_properties.shape.fluidity_coefficient == 0.9
        assert deserialized_entry.data.cat_properties.shape.signature == None

    def test_serialize_deserialize_only_unversioned(
        self,
        test_us_manager: USManagerBase,
        test_serializer: TestSerializer,
    ) -> None:
        # Create a test entry with only unversioned cat_properties fields having values
        entry = TestUSEntry(
            uuid="test-id",
            us_manager=test_us_manager,
            data=TestUSEntry.DataModel(
                secret_field="secret_value",
                unversioned_field="unversioned_value",
                cat_properties=CatProperties(
                    last_fed=1234567890,  # unversioned
                    color=None,  # versioned but None
                    name=None,  # versioned but None
                    shape=CatShapeProperties(
                        mass=5.5,  # unversioned
                        radius=10,  # unversioned
                        is_spherical=None,  # versioned but None
                        fluidity_coefficient=None,  # versioned but None
                        signature="blue paws",  # secret
                    ),
                ),
            ),
            data_strict=False,
        )

        # Serialize
        data_pack = test_serializer.serialize(entry)

        # Verify that unversioned fields are in unversioned_data
        assert "unversioned_field" in data_pack.unversioned_data.unversioned_data
        assert data_pack.unversioned_data.unversioned_data["unversioned_field"] == "unversioned_value"
        assert "cat_properties" in data_pack.unversioned_data.unversioned_data
        assert data_pack.unversioned_data.unversioned_data["cat_properties"]["last_fed"] == 1234567890
        assert data_pack.unversioned_data.unversioned_data["cat_properties"]["shape"]["mass"] == 5.5
        assert data_pack.unversioned_data.unversioned_data["cat_properties"]["shape"]["radius"] == 10

        # Verify that None versioned fields are still in main data
        assert "cat_properties" in data_pack.data
        assert data_pack.data["cat_properties"]["color"] is None
        assert data_pack.data["cat_properties"]["name"] is None
        assert data_pack.data["cat_properties"]["shape"]["is_spherical"] is None
        assert data_pack.data["cat_properties"]["shape"]["fluidity_coefficient"] is None

        # Deserialize
        deserialized_entry = test_serializer.deserialize(
            cls=TestUSEntry,
            data_pack=data_pack,
            entry_id="test-id",
            us_manager=test_us_manager,
            common_properties={},
            data_strict=False,
        )

        # Verify that all data is correctly reconstructed
        assert deserialized_entry.data.secret_field == "secret_value"
        assert deserialized_entry.data.unversioned_field == "unversioned_value"
        assert deserialized_entry.data.cat_properties is not None
        assert deserialized_entry.data.cat_properties.last_fed == 1234567890
        assert deserialized_entry.data.cat_properties.color is None
        assert deserialized_entry.data.cat_properties.name is None
        assert deserialized_entry.data.cat_properties.shape is not None
        assert deserialized_entry.data.cat_properties.shape.mass == 5.5
        assert deserialized_entry.data.cat_properties.shape.radius == 10
        assert deserialized_entry.data.cat_properties.shape.is_spherical is None
        assert deserialized_entry.data.cat_properties.shape.fluidity_coefficient is None
        assert deserialized_entry.data.cat_properties.shape.signature == "blue paws"
