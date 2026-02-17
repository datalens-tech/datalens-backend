import json
from typing import Any

import pydantic


def parse_json_dict(value: Any) -> dict[str, str] | None:
    if value is None:
        return None

    if isinstance(value, dict):
        return value

    if isinstance(value, str):
        return json.loads(value)

    raise ValueError(f"Invalid JSON value: {value}")


json_dict_validator = pydantic.BeforeValidator(parse_json_dict)


__all__ = [
    "json_dict_validator",
    "parse_json_dict",
]
