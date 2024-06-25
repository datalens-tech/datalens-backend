import pytest

from dl_constants.enums import (
    RLSPatternType,
    RLSSubjectType,
)
from dl_rls.models import (
    RLSEntry,
    RLSSubject,
)
from dl_rls.rls import RLS


def _add_rls_restrictions(rls: RLS, field_guid: str, restrictions: list[dict]) -> None:
    for entry in restrictions:
        subject_type = entry.get("subject_type")
        subject_id = entry.get("subject_id")

        if subject_type == RLSSubjectType.all:
            assert subject_id is None
            subject_id = "*"
        else:
            assert subject_id is not None
            assert subject_type is None
            subject_type = RLSSubjectType.user

        rls.items.append(
            RLSEntry(
                field_guid=field_guid,
                allowed_value=entry.get("allowed_value"),
                pattern_type=entry.get("pattern_type", RLSPatternType.value),
                subject=RLSSubject(
                    subject_type=subject_type,
                    subject_name=subject_id,
                    subject_id=subject_id,
                ),
            )
        )


def test_rls_simple():
    rls = RLS()
    field_guid = "guid"
    assert not rls.has_restrictions
    _add_rls_restrictions(rls, field_guid=field_guid, restrictions=[dict(allowed_value="QQQ", subject_id="qwerty")])

    assert rls.has_restrictions
    allow_all_values, allow_userid, allowed_values = rls.get_field_restriction_for_subject(
        field_guid=field_guid,
        subject_id="qwerty",
        subject_type=RLSSubjectType.user,
    )
    assert not allow_all_values
    assert not allow_userid
    assert allowed_values == ["QQQ"]


@pytest.mark.parametrize(
    "entrysets, expected_restrictions",
    [
        pytest.param(
            {"fld1": [dict(allowed_value="value_1", subject_id="user_1")]},
            {"user_1": {"fld1": ["value_1"]}},
            id="One subject, one field, one value",
        ),
        pytest.param(
            {
                "fld1": [
                    dict(allowed_value="value_1", subject_id="user_1"),
                    dict(allowed_value="value_2", subject_id="user_1"),
                ],
            },
            {"user_1": {"fld1": ["value_1", "value_2"]}},
            id="One subject, one field, multiple values",
        ),
        pytest.param(
            {
                "fld1": [
                    dict(allowed_value="value_1", subject_id="user_1"),
                    dict(allowed_value="value_2", subject_id="user_1"),
                ],
                "fld2": [
                    dict(allowed_value="value_3", subject_id="user_1"),
                ],
            },
            {"user_1": {"fld1": ["value_1", "value_2"], "fld2": ["value_3"]}},
            id="One subject, two fields",
        ),
        pytest.param(
            {"fld1": [dict(allowed_value="value", subject_id="not_user_1")]},
            {"user_1": {"fld1": []}},
            id="No values for subject",
        ),
        pytest.param(
            {
                "fld1": [
                    dict(allowed_value="value", subject_id="user_1"),
                    dict(allowed_value="value", subject_id="user_2"),
                ],
            },
            {
                "user_1": {"fld1": ["value"]},
                "user_2": {"fld1": ["value"]},
            },
            id="Two subjects, one field, one value",
        ),
        pytest.param(
            {
                "fld1": [
                    dict(allowed_value="value_1", subject_id="user_1"),
                    dict(allowed_value="value_1", subject_id="user_2"),
                ],
                "fld2": [
                    dict(allowed_value="value_2", subject_id="user_1"),
                ],
            },
            {
                "user_1": {"fld1": ["value_1"], "fld2": ["value_2"]},
                "user_2": {"fld1": ["value_1"], "fld2": []},
            },
            id="Two subjects, two fields",
        ),
        pytest.param(
            {
                "fld1": [
                    dict(pattern_type=RLSPatternType.all, subject_id="user_1"),
                    dict(allowed_value="value_1", subject_id="user_2"),
                ],
                "fld2": [
                    dict(allowed_value="value_2", subject_id="user_2"),
                ],
            },
            {
                "user_1": {"fld2": []},
                "user_2": {"fld1": ["value_1"], "fld2": ["value_2"]},
            },
            id="Value wildcard",
        ),
        pytest.param(
            {
                "fld1": [
                    dict(allowed_value="value_for_all", subject_type=RLSSubjectType.all),
                    dict(allowed_value="value_1", subject_id="user_1"),
                ],
                "fld2": [
                    dict(allowed_value="value_2", subject_id="user_2"),
                ],
            },
            {
                "user_1": {"fld1": ["value_for_all", "value_1"], "fld2": []},
                "user_2": {"fld1": ["value_for_all"], "fld2": ["value_2"]},
            },
            id="Subject wildcard",
        ),
        pytest.param(
            {
                "fld1": [
                    dict(allowed_value="f1_val_1_for_all", subject_type=RLSSubjectType.all),
                    dict(allowed_value="f1_val_2_for_all", subject_type=RLSSubjectType.all),
                    dict(pattern_type=RLSPatternType.all, subject_id="user_su_1"),
                    dict(pattern_type=RLSPatternType.all, subject_id="user_su_2"),
                    dict(allowed_value="f1_val_for_u3_u4", subject_id="user_3"),
                    dict(allowed_value="f1_val_for_u3_u4", subject_id="user_4"),
                    dict(allowed_value="f1_val_for_u5", subject_id="user_5"),
                ],
                "fld2": [
                    dict(allowed_value="f2_val_1_for_all", subject_type=RLSSubjectType.all),
                    dict(allowed_value="f2_val_for_su2", subject_id="user_su_2"),
                    dict(allowed_value="f2_val_for_u4", subject_id="user_4"),
                ],
            },
            {
                "user_su_1": {"fld2": ["f2_val_1_for_all"]},  # no fld1 filter
                "user_su_2": {"fld2": ["f2_val_1_for_all", "f2_val_for_su2"]},  # no fld1 filter
                "user_3": {
                    "fld1": ["f1_val_1_for_all", "f1_val_2_for_all", "f1_val_for_u3_u4"],
                    "fld2": ["f2_val_1_for_all"],
                },
                "user_4": {
                    "fld1": ["f1_val_1_for_all", "f1_val_2_for_all", "f1_val_for_u3_u4"],
                    "fld2": ["f2_val_1_for_all", "f2_val_for_u4"],
                },
                "user_5": {
                    "fld1": ["f1_val_1_for_all", "f1_val_2_for_all", "f1_val_for_u5"],
                    "fld2": ["f2_val_1_for_all"],
                },
            },
            id="Complex",
        ),
    ],
)
def test_rls(entrysets: dict, expected_restrictions: dict):
    rls = RLS()
    assert not rls.has_restrictions
    for field_guid, entries in entrysets.items():
        _add_rls_restrictions(rls, field_guid, entries)

    assert rls.has_restrictions
    restrictions = {
        user_id: rls.get_subject_restrictions(subject_type=RLSSubjectType.user, subject_id=user_id)
        for user_id in expected_restrictions.keys()
    }
    # Map back fields to aliases for readability
    restrictions = {
        user_id: {
            field_guid: field_restrictions for field_guid, field_restrictions in user_restrictions.items()
        }
        for user_id, user_restrictions in restrictions.items()
    }
    assert restrictions == expected_restrictions
