import pytest

from dl_cls.models import (
    CLSMaskSpec,
    CLSRule,
    CLSSubject,
    FieldCLS,
)
from dl_constants.enums import (
    CLSMode,
    CLSSubjectType,
)


def test_mask_spec_defaults() -> None:
    spec = CLSMaskSpec(mode=CLSMode.full)
    assert spec.mask == "******"
    assert spec.prefix == 0
    assert spec.suffix == 0
    assert spec.keep_length is False


def test_field_cls_default_rule_is_full() -> None:
    field_cls = FieldCLS()
    assert field_cls.default_rule == CLSMaskSpec(mode=CLSMode.full)


def test_field_cls_rules_default_empty() -> None:
    field_cls = FieldCLS()
    assert field_cls.rules == ()


def test_field_cls_rules_coerced_to_tuple() -> None:
    rule = CLSRule(
        subject=CLSSubject(subject_type=CLSSubjectType.user, subject_id="u1"),
        spec=CLSMaskSpec(mode=CLSMode.none),
    )
    field_cls = FieldCLS(rules=[rule])
    assert isinstance(field_cls.rules, tuple)
    assert field_cls.rules == (rule,)


def test_field_cls_is_hashable() -> None:
    field_cls = FieldCLS(
        default_rule=CLSMaskSpec(mode=CLSMode.full),
        rules=[
            CLSRule(
                subject=CLSSubject(subject_type=CLSSubjectType.group, subject_id="g1"),
                spec=CLSMaskSpec(mode=CLSMode.partial, prefix=1, suffix=1, keep_length=True),
            ),
        ],
    )
    assert isinstance(hash(field_cls), int)


def test_equal_field_cls_hash_equal() -> None:
    def build() -> FieldCLS:
        return FieldCLS(
            rules=[
                CLSRule(
                    subject=CLSSubject(subject_type=CLSSubjectType.user, subject_id="u1"),
                    spec=CLSMaskSpec(mode=CLSMode.full),
                ),
            ],
        )

    assert build() == build()
    assert hash(build()) == hash(build())


def test_subject_is_hashable() -> None:
    subject = CLSSubject(subject_type=CLSSubjectType.user, subject_id="u1")
    assert isinstance(hash(subject), int)
    assert subject == CLSSubject(subject_type=CLSSubjectType.user, subject_id="u1")


def test_negative_prefix_rejected() -> None:
    # Defense-in-depth: a negative edge would slice (e.g. value[:-1]) and leak source data,
    # even when a spec is built programmatically (bypassing the schema's Range validator).
    with pytest.raises(ValueError):
        CLSMaskSpec(mode=CLSMode.partial, prefix=-1)


def test_negative_suffix_rejected() -> None:
    with pytest.raises(ValueError):
        CLSMaskSpec(mode=CLSMode.partial, suffix=-1)


@pytest.mark.parametrize("mode", [CLSMode.partial, CLSMode.full])
def test_empty_mask_rejected_for_masking_modes(mode: CLSMode) -> None:
    # An empty mask leaks edges (partial) or breaks the length-preservation contract.
    with pytest.raises(ValueError):
        CLSMaskSpec(mode=mode, mask="")


def test_empty_mask_allowed_for_none_mode() -> None:
    # `none` never masks, so an (unused) empty mask is harmless.
    assert CLSMaskSpec(mode=CLSMode.none, mask="").mask == ""
