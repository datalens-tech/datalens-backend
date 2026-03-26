import attrs


def field_must_be_positive(
    _instance: object,
    attribute: attrs.Attribute,
    value: int,
) -> None:
    label = attribute.name
    if value <= 0:
        raise ValueError(f"{label} must be positive")
