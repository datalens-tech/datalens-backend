from marshmallow import ValidationError
import pytest

from dl_cls.models import (
    CLSMaskSpec,
    CLSRule,
    CLSSubject,
    FieldCLS,
)
from dl_cls.schema import FieldCLSSchema
from dl_constants.enums import (
    CLSMode,
    CLSSubjectType,
)


def test_round_trip_multitier() -> None:
    field_cls = FieldCLS(
        default_rule=CLSMaskSpec(mode=CLSMode.full, mask="X"),
        rules=[
            CLSRule(
                subject=CLSSubject(subject_type=CLSSubjectType.user, subject_id="u1"),
                spec=CLSMaskSpec(mode=CLSMode.none),
            ),
            CLSRule(
                subject=CLSSubject(subject_type=CLSSubjectType.group, subject_id="g1"),
                spec=CLSMaskSpec(mode=CLSMode.partial, mask="*", prefix=1, suffix=2, keep_length=True),
            ),
        ],
    )
    schema = FieldCLSSchema()
    assert schema.load(schema.dump(field_cls)) == field_cls


def test_round_trip_defaults() -> None:
    field_cls = FieldCLS()
    schema = FieldCLSSchema()
    assert schema.load(schema.dump(field_cls)) == field_cls


def test_keep_length_round_trips() -> None:
    field_cls = FieldCLS(
        default_rule=CLSMaskSpec(mode=CLSMode.partial, prefix=2, suffix=1, mask="*", keep_length=True),
    )
    schema = FieldCLSSchema()
    loaded = schema.load(schema.dump(field_cls))
    assert loaded == field_cls
    assert loaded.default_rule.keep_length is True


def test_load_minimal_payload_uses_defaults() -> None:
    loaded = FieldCLSSchema().load({"rules": []})
    assert loaded == FieldCLS()


def test_default_rule_required_when_rules_present() -> None:
    # A field that configures any CLS rule MUST declare its everyone-policy (default_rule) explicitly.
    payload = {
        "rules": [
            {
                "subject": {"subject_type": "user", "subject_id": "u1"},
                "spec": {"mode": "none"},
            },
        ],
    }
    with pytest.raises(ValidationError):
        FieldCLSSchema().load(payload)


def test_default_rule_optional_when_no_rules() -> None:
    # No rules -> no everyone-policy needed; the model's safe `full` default applies.
    loaded = FieldCLSSchema().load({"rules": []})
    assert loaded.default_rule.mode == CLSMode.full


def test_reject_partial_params_on_full_default_rule() -> None:
    payload = {
        "default_rule": {"mode": "full", "prefix": 2},
        "rules": [],
    }
    with pytest.raises(ValidationError):
        FieldCLSSchema().load(payload)


def test_keep_length_allowed_on_full() -> None:
    # `full` supports length preservation too (mask filled to the value's length).
    loaded = FieldCLSSchema().load({"default_rule": {"mode": "full", "keep_length": True}, "rules": []})
    assert loaded.default_rule.mode == CLSMode.full
    assert loaded.default_rule.keep_length is True


def test_reject_keep_length_on_none() -> None:
    # `none` never masks, so keep_length is meaningless there and rejected.
    payload = {
        "default_rule": {"mode": "none", "keep_length": True},
        "rules": [],
    }
    with pytest.raises(ValidationError):
        FieldCLSSchema().load(payload)


def test_reject_empty_mask_for_partial() -> None:
    # keep_length=True with an empty mask cannot preserve length and reveals edges with no fill.
    payload = {
        "default_rule": {"mode": "partial", "mask": "", "prefix": 1, "suffix": 1, "keep_length": True},
        "rules": [],
    }
    with pytest.raises(ValidationError):
        FieldCLSSchema().load(payload)


def test_reject_empty_mask_for_full() -> None:
    payload = {"default_rule": {"mode": "full", "mask": ""}, "rules": []}
    with pytest.raises(ValidationError):
        FieldCLSSchema().load(payload)


@pytest.mark.parametrize("field", ["prefix", "suffix"])
def test_reject_negative_partial_edge_lengths(field: str) -> None:
    payload = {
        "default_rule": {"mode": "partial", "mask": "*", field: -1},
        "rules": [],
    }
    with pytest.raises(ValidationError):
        FieldCLSSchema().load(payload)


def test_reject_partial_params_in_rule_spec() -> None:
    payload = {
        "default_rule": {"mode": "full"},
        "rules": [
            {
                "subject": {"subject_type": "user", "subject_id": "u1"},
                "spec": {"mode": "none", "suffix": 3},
            },
        ],
    }
    with pytest.raises(ValidationError):
        FieldCLSSchema().load(payload)
