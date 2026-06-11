from collections.abc import (
    Callable,
    Collection,
    Sequence,
)

from dl_cls.exc import CLSError
from dl_cls.models import (
    CLSMaskSpec,
    FieldCLS,
)
from dl_constants.enums import (
    CLSMode,
    CLSSubjectType,
)

# Explicit, exhaustive map — NOT enum order: reordering CLSMode must never change which rule wins.
_CLS_STRICTNESS: dict[CLSMode, int] = {
    CLSMode.none: 0,
    CLSMode.partial: 1,
    CLSMode.full: 2,
}


def _fill(fill: str, length: int) -> str:
    """Repeat ``fill`` to exactly ``length`` characters (truncating the final repeat)."""
    if length <= 0 or not fill:
        return ""
    return (fill * (length // len(fill) + 1))[:length]


def make_masker(spec: CLSMaskSpec) -> Callable[[str | None], str | None]:
    mode = spec.mode

    if mode == CLSMode.none:

        def mask_none(value: str | None) -> str | None:
            return value

        return mask_none

    if mode == CLSMode.full:
        mask = spec.mask
        keep_length = spec.keep_length

        def mask_full(value: str | None) -> str | None:
            if value is None:
                return None
            # keep_length fills the mask to the value's length; otherwise a fixed token.
            return _fill(mask, len(value)) if keep_length else mask

        return mask_full

    if mode == CLSMode.partial:
        prefix = spec.prefix
        suffix = spec.suffix
        mask = spec.mask
        keep_length = spec.keep_length

        def mask_partial(value: str | None) -> str | None:
            if value is None:
                return None
            length = len(value)
            # Keeping the edges would reveal the whole value (or more) — hide everything.
            if length <= prefix + suffix:
                return _fill(mask, length) if keep_length else mask
            head = value[:prefix]
            tail = value[length - suffix :] if suffix > 0 else ""
            if keep_length:
                # Fill the hidden middle so the output length equals the input length.
                return head + _fill(mask, length - prefix - suffix) + tail
            # Without length preservation, a value no longer than the mask is replaced wholesale (no leak).
            if length <= len(mask):
                return mask
            return head + mask + tail

        return mask_partial

    raise CLSError(f"Unsupported CLS masking mode: {mode}")


def _strictness_key(spec: CLSMaskSpec) -> tuple[int, int, int]:
    """Total strictness order, larger = stricter: mode map (``none < partial < full``), then a
    length-preservation tie-break for ``partial``/``full`` (refined by revealed edges for ``partial``).
    """
    base = _CLS_STRICTNESS[spec.mode]
    if spec.mode == CLSMode.partial:
        return (base, 0 if spec.keep_length else 1, -(spec.prefix + spec.suffix))
    if spec.mode == CLSMode.full:
        # full ignores prefix/suffix, so only the length-preservation tie-break applies.
        return (base, 0 if spec.keep_length else 1, 0)
    return (base, 0, 0)


def _strictest(specs: Sequence[CLSMaskSpec]) -> CLSMaskSpec:
    return max(specs, key=_strictness_key)


def select_effective_rule(
    field_cls: FieldCLS,
    user_id: str,
    allowed_groups: Collection[str],
) -> CLSMaskSpec:
    """Resolve the masking spec for a request via strictest-wins tiering.

    Tiers are checked in order: concrete user, then groups, then the field default. The first
    non-empty subject tier wins; within it the strictest spec is chosen. There is no ``all`` tier —
    ``default_rule`` is the sole "everyone" fallback. A field with no matching rule falls back to
    ``default_rule`` (full by default) — never to an unmasked result.
    """
    user_specs = [
        rule.spec
        for rule in field_cls.rules
        if rule.subject.subject_type == CLSSubjectType.user and rule.subject.subject_id == user_id
    ]
    if user_specs:
        return _strictest(user_specs)

    group_specs = [
        rule.spec
        for rule in field_cls.rules
        if rule.subject.subject_type == CLSSubjectType.group and rule.subject.subject_id in allowed_groups
    ]
    if group_specs:
        return _strictest(group_specs)

    return field_cls.default_rule
