import copy

from dl_utils.utils import (
    AddressableData,
    DataKey,
)


def test_pop_empty_with_empty_key() -> None:
    data = {
        "a": {
            "b": {
                "c": "value",
            },
        },
    }
    original_data = copy.deepcopy(data)

    AddressableData._pop_empty(data, DataKey(parts=()))

    assert data == original_data


def test_pop_empty_single_level_empty_dict() -> None:
    data = {
        "a": {},
        "b": "value",
    }

    AddressableData._pop_empty(data, DataKey(parts=("a",)))

    assert data == {
        "b": "value",
    }


def test_pop_empty_single_level() -> None:
    data = {
        "a": "string_value",
        "b": 123,
        "c": [1, 2, 3],
        "d": {
            "e": "string",
        },
    }
    original_data = copy.deepcopy(data)

    AddressableData._pop_empty(data, DataKey(parts=("a",)))
    AddressableData._pop_empty(data, DataKey(parts=("b",)))
    AddressableData._pop_empty(data, DataKey(parts=("c",)))
    AddressableData._pop_empty(
        data,
        DataKey(
            parts=(
                "d",
                "e",
            )
        ),
    )

    assert data == original_data


def test_pop_empty_nested_empty_dicts() -> None:
    data = {
        "a": {
            "b": {
                "c": {},
            },
        },
        "d": "value",
    }

    AddressableData._pop_empty(data, DataKey(parts=("a", "b", "c")))

    assert data == {
        "d": "value",
    }


def test_pop_empty_partial_nested_cleanup() -> None:
    data = {
        "a": {
            "b": {
                "c": {},
                "d": "value",
            },
        },
        "e": "value",
    }

    AddressableData._pop_empty(data, DataKey(parts=("a", "b", "c")))

    expected = {
        "a": {
            "b": {
                "d": "value",
            },
        },
        "e": "value",
    }
    assert data == expected


def test_pop_empty_multiple_branches() -> None:
    data = {
        "branch1": {
            "empty": {},
        },
        "branch2": {
            "also_empty": {},
        },
        "branch3": {"not_empty": "value"},
    }

    AddressableData._pop_empty(data, DataKey(parts=("branch1", "empty")))

    expected = {
        "branch2": {
            "also_empty": {},
        },
        "branch3": {"not_empty": "value"},
    }
    assert data == expected


def test_pop_empty_nonexistent_key() -> None:
    data = {
        "a": {
            "b": "value",
        },
    }
    original_data = copy.deepcopy(data)

    AddressableData._pop_empty(data, DataKey(parts=("nonexistent",)))

    assert data == original_data


def test_pop_empty_nonexistent_nested_key() -> None:
    data = {
        "a": {
            "b": "value",
        },
    }
    original_data = copy.deepcopy(data)

    AddressableData._pop_empty(data, DataKey(parts=("a", "nonexistent")))

    assert data == original_data


def test_pop_empty_integration_with_addressable_data() -> None:
    addressable_data = AddressableData(
        data={
            "section1": {
                "subsection": {
                    "item": "to_be_removed",
                },
            },
            "section2": "value",
        }
    )

    result = addressable_data.pop(DataKey(parts=("section1", "subsection", "item")), remove_empty=True)

    assert result == "to_be_removed"
    assert addressable_data.data == {
        "section2": "value",
    }


def test_addressable_data_ensure_objects_single() -> None:
    addressable_data = AddressableData({})

    addressable_data._ensure_objects(
        DataKey(parts=("test",)),
    )

    assert addressable_data.data == {}


def test_addressable_data_ensure_objects_nested() -> None:
    addressable_data = AddressableData({})

    addressable_data._ensure_objects(
        DataKey(parts=("a", "b", "c")),
    )

    assert addressable_data.data == {
        "a": {
            "b": {},
        },
    }


def test_addressable_data_ensure_objects_invalid_type_ignored() -> None:
    addressable_data = AddressableData(
        {
            "test": 13,
        }
    )

    addressable_data._ensure_objects(
        DataKey(parts=("test", "invalid")),
    )

    assert addressable_data.data == {
        "test": 13,
    }
