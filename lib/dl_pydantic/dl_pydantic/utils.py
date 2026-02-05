from typing import Any


def _check_duplicated_case_insensitive_fields_from_dicts(target: dict[str, Any], source: dict[str, Any]) -> None:
    target_with_lower_keys = {key.lower(): value for key, value in target.items()}
    for source_key in source:
        if source_key.lower() in target_with_lower_keys:
            raise ValueError(f"Can't merge duplicated field '{source_key}'")


def _merge_dict_keys(data: dict[str, Any]) -> dict[str, Any]:
    """
    Merge keys that differ only by case into a single lowercase key.
    For example: {'CHILD': {'VALUE': 'test_4'}, 'child': {'secret': 'secret_test'}}
    becomes: {'child': {'VALUE': 'test_4', 'secret': 'secret_test'}}

    Returns a new dictionary with merged keys.
    """
    result: dict[str, Any] = {}
    for key, value in data.items():
        key_lower = key.lower()
        if key_lower not in result:
            result[key_lower] = value
            continue
        if not isinstance(result[key_lower], dict) or not isinstance(value, dict):
            raise ValueError("Can't merge non-dict")
        _check_duplicated_case_insensitive_fields_from_dicts(result[key_lower], value)
        result[key_lower].update(value)

    return result
