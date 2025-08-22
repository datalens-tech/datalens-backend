import uuid

import pytest

from dl_core.components.ids import (
    ID_LENGTH,
    ID_VALID_SYMBOLS,
    RANDOM_STR_LENGTH,
    FieldIdValidator,
    ReadableFieldIdGenerator,
    ReadableFieldIdGeneratorWithPrefix,
    ReadableFieldIdGeneratorWithSuffix,
    make_readable_field_id,
)


def test_make_readable_field_id():
    assert make_readable_field_id(title="hello!:*", valid_symbols=ID_VALID_SYMBOLS, max_length=ID_LENGTH) == "hello"
    assert (
        make_readable_field_id(
            title=" René François Lacôte   123", valid_symbols=ID_VALID_SYMBOLS, max_length=ID_LENGTH
        )
        == "rene_francois_lacote_123"
    )
    assert make_readable_field_id(title="Проверка", valid_symbols=ID_VALID_SYMBOLS, max_length=ID_LENGTH) == "proverka"


@pytest.fixture
def validator():
    return FieldIdValidator()


@pytest.fixture
def dataset():
    class Field:
        def __init__(self, guid):
            self.guid = guid

    class ResultSchema:
        def __init__(self):
            self.fields = []

    class Dataset:
        def __init__(self):
            self.result_schema = ResultSchema()

        def add_field(self, guid):
            self.result_schema.fields.append(Field(guid))

    return Dataset()


class TestReadableFieldIdGenerator:
    def test_with_title(self, dataset, validator):
        generator = ReadableFieldIdGenerator(dataset=dataset)

        field_id = generator.make_field_id(title="Test Field")

        assert validator.is_valid(field_id)
        assert field_id == "test_field"

    def test_with_none_title(self, dataset, validator):
        generator = ReadableFieldIdGenerator(dataset=dataset)

        field_id = generator.make_field_id(title=None)

        assert validator.is_valid(field_id)
        try:
            uuid.UUID(field_id)
            is_uuid = True
        except ValueError:
            is_uuid = False
        assert is_uuid

    def test_with_special_chars(self, dataset, validator):
        generator = ReadableFieldIdGenerator(dataset=dataset)

        field_id = generator.make_field_id(title="Test!@#$%^&*()_+Field")

        assert validator.is_valid(field_id)
        assert field_id == "test_field"

    def test_with_long_title(self, dataset, validator):
        generator = ReadableFieldIdGenerator(dataset=dataset)

        long_title = "This is a very long title that should be truncated to the maximum length allowed for field IDs"
        field_id = generator.make_field_id(title=long_title)

        assert validator.is_valid(field_id)
        assert len(field_id) == generator._id_length
        assert field_id == "this_is_a_very_long_title_that_shoul"

    def test_resolve_id_collisions(self, dataset, validator):
        generator = ReadableFieldIdGenerator(dataset=dataset)

        dataset.add_field("test_field")
        field_id = generator.make_field_id(title="Test Field")

        assert validator.is_valid(field_id)
        assert field_id == "test_field_1"


class TestReadableFieldIdGeneratorWithPrefix:
    def test_with_none_title(self, dataset, validator):
        generator = ReadableFieldIdGeneratorWithPrefix(dataset=dataset)

        field_id = generator.make_field_id(title=None)

        assert validator.is_valid(field_id)
        try:
            uuid.UUID(field_id)
            is_uuid = True
        except ValueError:
            is_uuid = False
        assert is_uuid

    def test_with_prefix(self, dataset, validator):
        generator = ReadableFieldIdGeneratorWithPrefix(dataset=dataset)

        field_id = generator.make_field_id(title="Test Field")
        parts = field_id.split("_", 1)

        assert validator.is_valid(field_id)
        assert len(parts[0]) == RANDOM_STR_LENGTH
        assert parts[1] == "test_field"

    def test_with_long_title(self, dataset, validator):
        generator = ReadableFieldIdGeneratorWithPrefix(dataset=dataset)

        long_title = "This is a very long title that should be truncated to the maximum length allowed for field IDs"
        field_id = generator.make_field_id(title=long_title)
        parts = field_id.split("_", 1)

        assert validator.is_valid(field_id)
        assert parts[1] == "this_is_a_very_long_title_that_"
        assert len(field_id) == generator._id_length + RANDOM_STR_LENGTH + 1

    def test_resolve_id_collisions(self, dataset, validator):
        generator = ReadableFieldIdGeneratorWithPrefix(dataset=dataset)

        field_id = generator.make_field_id(title="Test Field")
        dataset.add_field(field_id)
        new_field_id = generator.make_field_id(title="Test Field")

        assert validator.is_valid(new_field_id)
        assert new_field_id != field_id


class TestReadableFieldIdGeneratorWithSuffix:
    def test_with_none_title(self, dataset, validator):
        generator = ReadableFieldIdGeneratorWithSuffix(dataset=dataset)

        field_id = generator.make_field_id(title=None)

        assert validator.is_valid(field_id)
        try:
            uuid.UUID(field_id)
            is_uuid = True
        except ValueError:
            is_uuid = False
        assert is_uuid

    def test_with_suffix(self, dataset, validator):
        generator = ReadableFieldIdGeneratorWithSuffix(dataset=dataset)

        field_id = generator.make_field_id(title="Test Field")
        parts = field_id.rsplit("_", 1)

        assert validator.is_valid(field_id)
        assert len(parts[1]) == RANDOM_STR_LENGTH
        assert parts[0] == "test_field"

    def test_with_long_title(self, dataset, validator):
        generator = ReadableFieldIdGeneratorWithSuffix(dataset=dataset)

        long_title = "This is a very long title that should be truncated to the maximum length allowed for field IDs"
        field_id = generator.make_field_id(title=long_title)
        parts = field_id.rsplit("_", 1)

        assert validator.is_valid(field_id)
        assert parts[0] == "this_is_a_very_long_title_that_"
        assert len(field_id) == generator._id_length + RANDOM_STR_LENGTH + 1

    def test_resolve_id_collisions(self, dataset, validator):
        generator = ReadableFieldIdGeneratorWithSuffix(dataset=dataset)

        field_id = generator.make_field_id(title="Test Field")
        dataset.add_field(field_id)
        new_field_id = generator.make_field_id(title="Test Field")

        assert validator.is_valid(new_field_id)
        assert new_field_id != field_id
