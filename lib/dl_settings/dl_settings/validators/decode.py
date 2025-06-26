import pydantic


def decode_multiline(value: str) -> str:
    return value.replace("\\n", "\n")


decode_multiline_validator = pydantic.BeforeValidator(decode_multiline)


__all__ = [
    "decode_multiline",
    "decode_multiline_validator",
]
