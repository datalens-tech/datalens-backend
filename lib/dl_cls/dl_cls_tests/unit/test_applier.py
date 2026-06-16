import pytest

from dl_cls.applier import (
    _CLS_STRICTNESS,
    _strictness_key,
    make_masker,
    select_effective_rule,
)
from dl_cls.models import (
    CLSMaskSpec,
    CLSRule,
    CLSSubject,
    FieldCLS,
)
from dl_constants import (
    CLSMode,
    CLSSubjectType,
)

# --- make_masker -------------------------------------------------------------


def test_none_mode_is_identity() -> None:
    masker = make_masker(CLSMaskSpec(mode=CLSMode.none))
    assert masker("secret") == "secret"


@pytest.mark.parametrize(
    "mode",
    [
        CLSMode.none,
        CLSMode.partial,
        CLSMode.full,
    ],
)
def test_none_value_passes_through(mode: CLSMode) -> None:
    masker = make_masker(CLSMaskSpec(mode=mode, prefix=1, suffix=1))
    assert masker(None) is None


def test_full_mode_default_mask() -> None:
    masker = make_masker(CLSMaskSpec(mode=CLSMode.full))
    assert masker("anything") == "******"


def test_full_mode_custom_mask() -> None:
    masker = make_masker(CLSMaskSpec(mode=CLSMode.full, mask="***"))
    assert masker("anything") == "***"


def test_full_mode_keep_length_preserves_length() -> None:
    masker = make_masker(CLSMaskSpec(mode=CLSMode.full, mask="*", keep_length=True))
    result = masker("john@example.com")  # 16 chars, fully hidden but same length
    assert result == "*" * 16
    assert len(result) == len("john@example.com")


def test_full_mode_keep_length_fills_default_mask_to_length() -> None:
    masker = make_masker(CLSMaskSpec(mode=CLSMode.full, keep_length=True))
    result = masker("john@example.com")  # default mask "******" repeated/truncated to 16
    assert result == "*" * 16
    assert len(result) == 16


def test_full_mode_without_keep_length_is_fixed_token() -> None:
    masker = make_masker(CLSMaskSpec(mode=CLSMode.full, mask="*", keep_length=False))
    assert masker("john@example.com") == "*"  # length not preserved


def test_partial_preserves_edges_hides_middle() -> None:
    masker = make_masker(CLSMaskSpec(mode=CLSMode.partial, prefix=2, suffix=2, mask="*"))
    assert masker("abcdefgh") == "ab*gh"


def test_partial_prefix_only() -> None:
    masker = make_masker(CLSMaskSpec(mode=CLSMode.partial, prefix=3, suffix=0, mask="*"))
    assert masker("abcdef") == "abc*"


def test_partial_suffix_only() -> None:
    masker = make_masker(CLSMaskSpec(mode=CLSMode.partial, prefix=0, suffix=2, mask="*"))
    assert masker("abcdef") == "*ef"


def test_partial_short_value_fully_masked() -> None:
    masker = make_masker(CLSMaskSpec(mode=CLSMode.partial, prefix=2, suffix=2, mask="*"))
    # len == prefix + suffix -> whole value replaced, no source characters leak
    assert masker("abcd") == "*"


def test_partial_value_shorter_than_mask_fully_masked() -> None:
    masker = make_masker(CLSMaskSpec(mode=CLSMode.partial, prefix=1, suffix=0, mask="[hidden]"))
    # not preserving length and len(value) <= len(mask) -> whole value replaced
    assert masker("abc") == "[hidden]"


def test_partial_keep_length_preserves_length() -> None:
    masker = make_masker(CLSMaskSpec(mode=CLSMode.partial, prefix=2, suffix=2, mask="*", keep_length=True))
    result = masker("abcdefgh")
    assert result == "ab****gh"
    assert len(result) == len("abcdefgh")


def test_partial_keep_length_fills_with_repeated_mask() -> None:
    # The fill repeats the mask string to the hidden span length, then truncates.
    masker = make_masker(CLSMaskSpec(mode=CLSMode.partial, prefix=1, suffix=1, mask="xy", keep_length=True))
    result = masker("ABCDEF")  # head "A" + 4-char fill from "xy" + tail "F"
    assert result == "AxyxyF"
    assert len(result) == len("ABCDEF")


def test_partial_keep_length_short_value_fully_masked_preserving_length() -> None:
    masker = make_masker(CLSMaskSpec(mode=CLSMode.partial, prefix=2, suffix=2, mask="*", keep_length=True))
    # whole value hidden, but length preserved
    assert masker("abcd") == "****"
    assert masker("ab") == "**"


def test_partial_no_keep_length_does_not_preserve_length() -> None:
    masker = make_masker(CLSMaskSpec(mode=CLSMode.partial, prefix=1, suffix=1, mask="*", keep_length=False))
    result = masker("abcdef")  # head "a" + mask "*" + tail "f"
    assert result == "a*f"
    assert len(result) != len("abcdef")


# --- select_effective_rule ---------------------------------------------------


def _rule(
    subject_type: CLSSubjectType,
    subject_id: str,
    mode: CLSMode,
    *,
    prefix: int = 0,
    suffix: int = 0,
    keep_length: bool = False,
) -> CLSRule:
    return CLSRule(
        subject=CLSSubject(subject_type=subject_type, subject_id=subject_id),
        spec=CLSMaskSpec(mode=mode, prefix=prefix, suffix=suffix, keep_length=keep_length),
    )


def test_user_tier_wins_over_group() -> None:
    field_cls = FieldCLS(
        rules=[
            _rule(CLSSubjectType.user, "u1", CLSMode.none),
            _rule(CLSSubjectType.group, "g1", CLSMode.full),
        ],
    )
    spec = select_effective_rule(field_cls, user_id="u1", allowed_groups={"g1"})
    assert spec.mode == CLSMode.none


def test_strictest_within_user_tier() -> None:
    field_cls = FieldCLS(
        rules=[
            _rule(CLSSubjectType.user, "u1", CLSMode.none),
            _rule(CLSSubjectType.user, "u1", CLSMode.full),
        ],
    )
    spec = select_effective_rule(field_cls, user_id="u1", allowed_groups=set())
    assert spec.mode == CLSMode.full


def test_group_tier_when_no_user_rule() -> None:
    field_cls = FieldCLS(
        default_rule=CLSMaskSpec(mode=CLSMode.full),
        rules=[_rule(CLSSubjectType.group, "g1", CLSMode.partial)],
    )
    spec = select_effective_rule(field_cls, user_id="u1", allowed_groups={"g1"})
    assert spec.mode == CLSMode.partial


def test_strictest_within_group_tier() -> None:
    field_cls = FieldCLS(
        rules=[
            _rule(CLSSubjectType.group, "g1", CLSMode.partial),
            _rule(CLSSubjectType.group, "g2", CLSMode.full),
        ],
    )
    spec = select_effective_rule(field_cls, user_id="u1", allowed_groups={"g1", "g2"})
    assert spec.mode == CLSMode.full


def test_no_all_tier_falls_through_to_default() -> None:
    # There is no `all` subject/tier: an unmatched request falls straight to the default rule.
    field_cls = FieldCLS(
        default_rule=CLSMaskSpec(mode=CLSMode.full),
        rules=[_rule(CLSSubjectType.group, "g1", CLSMode.none)],
    )
    spec = select_effective_rule(field_cls, user_id="nobody", allowed_groups=set())
    assert spec == field_cls.default_rule
    assert spec.mode == CLSMode.full


def test_empty_groups_fall_through_to_default() -> None:
    field_cls = FieldCLS(
        default_rule=CLSMaskSpec(mode=CLSMode.full),
        rules=[_rule(CLSSubjectType.group, "g1", CLSMode.none)],
    )
    spec = select_effective_rule(field_cls, user_id="u1", allowed_groups=set())
    assert spec.mode == CLSMode.full


def test_safe_default_on_no_match() -> None:
    field_cls = FieldCLS(default_rule=CLSMaskSpec(mode=CLSMode.full))
    spec = select_effective_rule(field_cls, user_id="u1", allowed_groups={"g1"})
    assert spec == field_cls.default_rule
    assert spec.mode == CLSMode.full


# --- strictest ordering within `partial` -------------------------------------


def test_no_length_preservation_is_stricter_than_length_preservation() -> None:
    # Two partial rules in one tier, identical except keep_length -> the no-preserve one wins.
    field_cls = FieldCLS(
        rules=[
            _rule(CLSSubjectType.user, "u1", CLSMode.partial, prefix=2, suffix=2, keep_length=True),
            _rule(CLSSubjectType.user, "u1", CLSMode.partial, prefix=2, suffix=2, keep_length=False),
        ],
    )
    spec = select_effective_rule(field_cls, user_id="u1", allowed_groups=set())
    assert spec.mode == CLSMode.partial
    assert spec.keep_length is False


def test_fewer_revealed_edges_is_stricter() -> None:
    # Among equal keep_length partial rules, fewer revealed edge chars (prefix+suffix) wins.
    field_cls = FieldCLS(
        rules=[
            _rule(CLSSubjectType.user, "u1", CLSMode.partial, prefix=2, suffix=2, keep_length=False),
            _rule(CLSSubjectType.user, "u1", CLSMode.partial, prefix=1, suffix=0, keep_length=False),
        ],
    )
    spec = select_effective_rule(field_cls, user_id="u1", allowed_groups=set())
    assert (spec.prefix, spec.suffix) == (1, 0)


def test_full_is_stricter_than_any_partial() -> None:
    field_cls = FieldCLS(
        rules=[
            _rule(CLSSubjectType.user, "u1", CLSMode.partial, prefix=0, suffix=0, keep_length=False),
            _rule(CLSSubjectType.user, "u1", CLSMode.full),
        ],
    )
    spec = select_effective_rule(field_cls, user_id="u1", allowed_groups=set())
    assert spec.mode == CLSMode.full


def test_full_no_length_preservation_is_stricter_than_length_preserving_full() -> None:
    # Within `full` too: keeping length leaks the value's length, so it is the less-strict choice.
    field_cls = FieldCLS(
        rules=[
            _rule(CLSSubjectType.user, "u1", CLSMode.full, keep_length=True),
            _rule(CLSSubjectType.user, "u1", CLSMode.full, keep_length=False),
        ],
    )
    spec = select_effective_rule(field_cls, user_id="u1", allowed_groups=set())
    assert spec.mode == CLSMode.full
    assert spec.keep_length is False


def test_full_strictness_ignores_prefix_suffix() -> None:
    # full ignores prefix/suffix in masking, so they must not affect its strictness either.
    bare = CLSMaskSpec(mode=CLSMode.full)
    with_edges = CLSMaskSpec(mode=CLSMode.full, prefix=5, suffix=5)
    assert _strictness_key(bare) == _strictness_key(with_edges)


def test_partial_selection_is_deterministic() -> None:
    # Several competing partial rules -> a single deterministic strictest winner regardless of order.
    strictest = _rule(CLSSubjectType.user, "u1", CLSMode.partial, prefix=0, suffix=0, keep_length=False)
    others = [
        _rule(CLSSubjectType.user, "u1", CLSMode.partial, prefix=3, suffix=3, keep_length=True),
        _rule(CLSSubjectType.user, "u1", CLSMode.partial, prefix=1, suffix=1, keep_length=False),
        _rule(CLSSubjectType.user, "u1", CLSMode.partial, prefix=0, suffix=0, keep_length=True),
    ]
    for ordering in ([strictest, *others], [*others, strictest]):
        spec = select_effective_rule(FieldCLS(rules=ordering), user_id="u1", allowed_groups=set())
        assert (spec.prefix, spec.suffix, spec.keep_length) == (0, 0, False)


def test_strictness_map_is_explicit_and_complete() -> None:
    assert _CLS_STRICTNESS[CLSMode.none] < _CLS_STRICTNESS[CLSMode.partial] < _CLS_STRICTNESS[CLSMode.full]
    # Every mode must carry an explicit strictness — adding a mode without one must fail loudly.
    assert set(_CLS_STRICTNESS) == set(CLSMode)
