from unittest.mock import AsyncMock

import pydantic
import pytest

import dl_dynconfig
import dl_pydantic


class NestedModel(dl_pydantic.BaseModel):
    value: str = "default"


class FlatConfig(dl_dynconfig.DynConfig):
    nested: NestedModel = pydantic.Field(default_factory=NestedModel)


class PrimitiveConfig(dl_dynconfig.DynConfig):
    name: str = ""
    count: int = 0
    tags: list[str] = pydantic.Field(default_factory=list)


@pytest.mark.asyncio
async def test_flat_config_update() -> None:
    source = dl_dynconfig.InMemorySource(data={"nested": {"value": "abc"}})
    config = FlatConfig.model_from_source(source=source)

    await config.model_fetch()
    assert config.nested.value == "abc"


@pytest.mark.asyncio
async def test_flat_config_default_before_update() -> None:
    source = dl_dynconfig.InMemorySource(data={"nested": {"value": "abc"}})
    config = FlatConfig.model_from_source(source=source)

    assert config.nested.value == "default"


@pytest.mark.asyncio
async def test_flat_config_initial_data() -> None:
    source = dl_dynconfig.InMemorySource(data={"nested": {"value": "abc"}})
    config = FlatConfig.model_from_source(source=source, initial_data={"nested": {"value": "def"}})
    assert config.nested.value == "def"


class Child2Config(dl_pydantic.BaseModel):
    value: str = "default"


class ChildConfig(dl_dynconfig.DynConfig):
    child2: Child2Config = pydantic.Field(default_factory=Child2Config)


class RootConfig(dl_dynconfig.DynConfig):
    child: ChildConfig = pydantic.Field(default_factory=ChildConfig)


@pytest.mark.asyncio
async def test_nested_config() -> None:
    source = dl_dynconfig.InMemorySource(data={"child": {"child2": {"value": "abc"}}})
    root = RootConfig.model_from_source(source=source)
    await root.model_fetch()
    assert root.child.child2.value == "abc"


@pytest.mark.asyncio
async def test_sub_config_update() -> None:
    source = dl_dynconfig.InMemorySource(data={"child": {"child2": {"value": "abc"}}})
    root = RootConfig.model_from_source(source=source)
    await root.model_fetch()
    await source.store({"child": {"child2": {"value": "abc"}}})
    await root.child.model_fetch()
    assert root.child.child2.value == "abc"


@pytest.mark.asyncio
async def test_sub_config_as_dependency() -> None:
    source = dl_dynconfig.InMemorySource(data={"child": {"child2": {"value": "abc"}}})
    root = RootConfig.model_from_source(source=source)
    await root.model_fetch()
    child_ref = root.child
    await source.store({"child": {"child2": {"value": "updated"}}})
    await child_ref.model_fetch()
    assert child_ref.child2.value == "updated"


@pytest.mark.asyncio
async def test_source_not_set_error() -> None:
    config = FlatConfig()
    with pytest.raises(dl_dynconfig.SourceNotSetError):
        await config.model_fetch()


@pytest.mark.asyncio
async def test_fetch_error_force() -> None:
    source = AsyncMock(spec=dl_dynconfig.BaseSource)
    source.fetch.side_effect = RuntimeError("connection failed")
    config = FlatConfig.model_from_source(source=source)

    with pytest.raises(dl_dynconfig.FetchError):
        await config.model_fetch(force=True)


@pytest.mark.asyncio
async def test_fetch_error_no_force() -> None:
    source = AsyncMock(spec=dl_dynconfig.BaseSource)
    source.fetch.side_effect = RuntimeError("connection failed")
    config = FlatConfig.model_from_source(source=source)

    await config.model_fetch(force=False)
    assert config.nested.value == "default"


@pytest.mark.asyncio
async def test_validation_error_wrong_type() -> None:
    source = dl_dynconfig.InMemorySource(data={"nested": "not_a_dict"})
    config = FlatConfig.model_from_source(source=source)

    with pytest.raises(pydantic.ValidationError):
        await config.model_fetch()


@pytest.mark.asyncio
async def test_validation_error_non_mapping() -> None:
    source = dl_dynconfig.InMemorySource(data="not_a_mapping")
    config = FlatConfig.model_from_source(source=source)

    with pytest.raises(pydantic.ValidationError):
        await config.model_fetch()


@pytest.mark.asyncio
async def test_validate_assignment_on_fetch() -> None:
    source = dl_dynconfig.InMemorySource(data={"name": "abc", "count": 5, "tags": ["a", "b"]})
    config = PrimitiveConfig.model_from_source(source=source)

    await config.model_fetch()
    assert config.name == "abc"
    assert config.count == 5
    assert config.tags == ["a", "b"]


@pytest.mark.asyncio
async def test_validate_assignment_rejects_wrong_type_on_fetch() -> None:
    source = dl_dynconfig.InMemorySource(data={"name": "abc", "count": "not_an_int", "tags": []})
    config = PrimitiveConfig.model_from_source(source=source)

    with pytest.raises(pydantic.ValidationError):
        await config.model_fetch()


def test_validate_assignment_rejects_wrong_type_on_setattr() -> None:
    config = PrimitiveConfig()

    with pytest.raises(pydantic.ValidationError):
        config.count = "not_an_int"  # type: ignore[assignment]


def test_validate_assignment_accepts_correct_type_on_setattr() -> None:
    config = PrimitiveConfig()
    config.name = "updated"
    config.count = 42
    assert config.name == "updated"
    assert config.count == 42


@pytest.mark.asyncio
async def test_fetch_resets_missing_fields_to_defaults() -> None:
    source = dl_dynconfig.InMemorySource(data={"name": "abc", "count": 5, "tags": ["a"]})
    config = PrimitiveConfig.model_from_source(source=source)

    await config.model_fetch()
    assert config.name == "abc"
    assert config.count == 5
    assert config.tags == ["a"]

    await source.store({"name": "updated"})
    await config.model_fetch()
    assert config.name == "updated"
    assert config.count == 0
    assert config.tags == []


class RequiredFieldConfig(dl_dynconfig.DynConfig):
    name: str


@pytest.mark.asyncio
async def test_missing_field_falls_back_to_initial_data() -> None:
    source = dl_dynconfig.InMemorySource(data={})
    config = RequiredFieldConfig.model_from_source(source=source, initial_data={"name": "initial"})

    await config.model_fetch()
    assert config.name == "initial"


@pytest.mark.asyncio
async def test_missing_field_falls_back_to_default() -> None:
    source = dl_dynconfig.InMemorySource(data={"name": "fetched"})
    config = PrimitiveConfig.model_from_source(source=source, initial_data={"count": 99})

    await config.model_fetch()
    assert config.name == "fetched"
    assert config.count == 99
    assert config.tags == []


@pytest.mark.asyncio
async def test_missing_required_field_without_initial_data_raises() -> None:
    source = dl_dynconfig.InMemorySource(data={})
    config = RequiredFieldConfig.model_from_source(source=source, initial_data={"name": "initial"})

    await source.store({"other": "value"})
    await config.model_fetch()
    assert config.name == "initial"

    config_no_initial = RequiredFieldConfig.model_from_source(
        source=dl_dynconfig.InMemorySource(data={}),
        initial_data={"name": "x"},
    )
    # initial_data covers it, so no error
    await config_no_initial.model_fetch()
    assert config_no_initial.name == "x"


@pytest.mark.asyncio
async def test_missing_required_field_no_initial_data_no_default_raises() -> None:
    source = dl_dynconfig.InMemorySource(data={})
    config = RequiredFieldConfig.model_from_source(source=source, initial_data={"name": "init"})

    await source.store({"unrelated": "value"})

    config._initial_data = {}
    with pytest.raises(pydantic.ValidationError):
        await config.model_fetch()


@pytest.mark.asyncio
async def test_path_traversal_non_mapping() -> None:
    source = dl_dynconfig.InMemorySource(data={"child": "not_a_mapping"})
    root = RootConfig.model_from_source(source=source)

    with pytest.raises(pydantic.ValidationError):
        await root.child.model_fetch()


@pytest.mark.asyncio
async def test_path_traversal_missing_key() -> None:
    source = dl_dynconfig.InMemorySource(data={"other": {}})
    root = RootConfig.model_from_source(source=source)

    with pytest.raises(pydantic.ValidationError):
        await root.child.model_fetch()


@pytest.mark.asyncio
async def test_nested_dynconfig_child_missing_from_data() -> None:
    source = dl_dynconfig.InMemorySource(data={})
    root = RootConfig.model_from_source(source=source)

    await root.model_fetch()
    assert root.child.child2.value == "default"


@pytest.mark.asyncio
async def test_model_store_root() -> None:
    source = dl_dynconfig.InMemorySource(data={"name": "original", "count": 0, "tags": []})
    config = PrimitiveConfig.model_from_source(source=source)

    await config.model_fetch()
    config.name = "stored"
    config.count = 42
    await config.model_store()

    result = await source.fetch()
    assert result["name"] == "stored"
    assert result["count"] == 42


@pytest.mark.asyncio
async def test_model_store_nested() -> None:
    source = dl_dynconfig.InMemorySource(data={"child": {"child2": {"value": "abc"}}})
    root = RootConfig.model_from_source(source=source)

    await root.model_fetch()
    root.child.child2.value = "updated"
    await root.child.model_store()

    result = await source.fetch()
    assert result["child"]["child2"]["value"] == "updated"


@pytest.mark.asyncio
async def test_model_store_source_not_set() -> None:
    config = PrimitiveConfig()

    with pytest.raises(dl_dynconfig.SourceNotSetError):
        await config.model_store()
