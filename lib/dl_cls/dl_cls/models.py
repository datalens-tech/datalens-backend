from collections.abc import Iterable
from typing import Any

import attrs

from dl_constants import (
    CLSMode,
    CLSSubjectType,
)


def _non_negative(instance: Any, attribute: attrs.Attribute, value: int) -> None:
    # A negative edge would slice (value[:-1]) and leak source data.
    if value < 0:
        raise ValueError(f"{attribute.name} must be non-negative, got {value}")


def _mask_non_empty(instance: Any, attribute: attrs.Attribute, value: str) -> None:
    # Empty mask would leak edges or fail to fill length; `none` never masks, so it is exempt.
    if instance.mode in (CLSMode.partial, CLSMode.full) and not value:
        raise ValueError("mask must be non-empty for 'partial' and 'full' modes")


@attrs.frozen
class CLSMaskSpec:
    mode: CLSMode
    # Repeatable, non-empty fill (asterisks) so length preservation is well-defined.
    mask: str = attrs.field(default="******", validator=_mask_non_empty)
    prefix: int = attrs.field(default=0, validator=_non_negative)
    suffix: int = attrs.field(default=0, validator=_non_negative)
    # `partial`/`full`: whether the masked output preserves the source value's length.
    keep_length: bool = False


@attrs.frozen
class CLSSubject:
    subject_type: CLSSubjectType
    subject_id: str


@attrs.frozen
class CLSRule:
    subject: CLSSubject
    spec: CLSMaskSpec


def _to_rules(rules: Iterable[CLSRule]) -> tuple[CLSRule, ...]:
    return tuple(rules)


@attrs.frozen
class FieldCLS:
    default_rule: CLSMaskSpec = attrs.field(factory=lambda: CLSMaskSpec(mode=CLSMode.full))
    # tuple (not list) so FieldCLS stays hashable — it lives inside the BIField cache key.
    rules: tuple[CLSRule, ...] = attrs.field(default=(), converter=_to_rules)
