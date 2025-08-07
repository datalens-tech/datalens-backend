import typing

import pytest

from dl_constants.enums import RLSSubjectType
from dl_rls.models import RLSSubject
from dl_rls.serializer import FieldRLSSerializer
from dl_rls.subject_resolver import NotFoundSubjectResolver
from dl_rls.testing.testing_data import (
    RLS_CONFIG_CASES,
    config_to_comparable,
)


SUBJECTS = ["@sa:123", "@group:gg", "user1", "user2", "@sa:456"]


def test_group_names_by_account_type() -> None:
    account_types = FieldRLSSerializer._group_subject_names_by_type(SUBJECTS)
    assert account_types == FieldRLSSerializer.AccountGroups(["user1", "user2"], ["@sa:123", "@sa:456"], ["@group:gg"])


def test_parse_sa_str() -> None:
    parsed = FieldRLSSerializer._parse_sa_str("@sa:123")
    assert parsed == RLSSubject(subject_type=RLSSubjectType.user, subject_id="123", subject_name="@sa:123")


def test_parse_group_str() -> None:
    parsed = FieldRLSSerializer._parse_group_str("@group:gg")
    assert parsed == RLSSubject(subject_type=RLSSubjectType.group, subject_id="gg", subject_name="@group:gg")


def test_resolve_subject_names() -> None:
    subject_resolver = NotFoundSubjectResolver()
    resolved_subjects = FieldRLSSerializer._resolve_subject_names(SUBJECTS, subject_resolver)
    assert resolved_subjects == {
        "user1": RLSSubject(subject_type=RLSSubjectType.notfound, subject_id="", subject_name="!FAILED_user1"),
        "user2": RLSSubject(subject_type=RLSSubjectType.notfound, subject_id="", subject_name="!FAILED_user2"),
        "@sa:123": RLSSubject(subject_type=RLSSubjectType.user, subject_id="123", subject_name="@sa:123"),
        "@sa:456": RLSSubject(subject_type=RLSSubjectType.user, subject_id="456", subject_name="@sa:456"),
        "@group:gg": RLSSubject(subject_type=RLSSubjectType.group, subject_id="gg", subject_name="@group:gg"),
    }


@pytest.mark.parametrize("case", RLS_CONFIG_CASES, ids=[typing.cast(str, c["name"]) for c in RLS_CONFIG_CASES])
def test_rls_entries_to_text_config(case: dict[str, typing.Any]) -> None:
    expected_config = case["config_to_compare"]
    rls_entries = case["rls_entries"]

    config = FieldRLSSerializer.to_text_config(rls_entries)

    assert config_to_comparable(config) == config_to_comparable(expected_config)
