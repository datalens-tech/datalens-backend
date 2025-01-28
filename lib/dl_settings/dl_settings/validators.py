def decode_multiline(value: str) -> str:
    return value.replace("\\n", "\n")


__all__ = [
    "decode_multiline",
]
