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
    original_data = data.copy()

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


def test_pop_empty_single_level_non_empty_dict() -> None:
    data = {
        "a": {
            "nested": "value",
        },
        "b": "value",
    }
    original_data = data.copy()

    AddressableData._pop_empty(data, DataKey(parts=("a",)))

    assert data == original_data


def test_pop_empty_single_level_non_dict_value() -> None:
    data = {
        "a": "string_value",
        "b": 123,
        "c": [1, 2, 3],
    }
    original_data = data.copy()

    AddressableData._pop_empty(data, DataKey(parts=("a",)))
    AddressableData._pop_empty(data, DataKey(parts=("b",)))
    AddressableData._pop_empty(data, DataKey(parts=("c",)))

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

    assert data == {"d": "value"}


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


def test_pop_empty_deep_nested_structure() -> None:
    data = {
        "level1": {
            "level2": {
                "level3": {
                    "level4": {
                        "level5": {},
                    },
                },
            },
        },
        "other": "value",
    }

    AddressableData._pop_empty(data, DataKey(parts=("level1", "level2", "level3", "level4", "level5")))

    assert data == {
        "other": "value",
    }


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
    original_data = data.copy()

    AddressableData._pop_empty(data, DataKey(parts=("nonexistent",)))

    assert data == original_data


def test_pop_empty_nonexistent_nested_key() -> None:
    data = {
        "a": {
            "b": "value",
        },
    }
    original_data = data.copy()

    AddressableData._pop_empty(data, DataKey(parts=("a", "nonexistent")))

    assert data == original_data


def test_pop_empty_stops_at_non_empty_parent() -> None:
    data = {
        "root": {
            "parent": {
                "child": {
                    "target": {},
                },
                "sibling": "value",
            },
        },
    }

    AddressableData._pop_empty(data, DataKey(parts=("root", "parent", "child", "target")))

    expected = {
        "root": {
            "parent": {
                "sibling": "value",
            },
        },
    }
    assert data == expected


def test_pop_empty_with_none_values() -> None:
    data = {
        "a": None,
        "b": {
            "c": {},
        },
    }

    # Should not crash when encountering None
    AddressableData._pop_empty(data, DataKey(parts=("a", "nonexistent")))

    # Should not pop None
    AddressableData._pop_empty(data, DataKey(parts=("a")))

    # Should still work for valid paths
    AddressableData._pop_empty(data, DataKey(parts=("b", "c")))

    expected = {
        "a": None,
    }
    assert data == expected


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
