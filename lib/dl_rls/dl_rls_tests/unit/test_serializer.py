import pytest

from dl_constants.enums import RLSSubjectType
from dl_rls.models import RLSSubject
from dl_rls.serializer import FieldRLSSerializer
from dl_rls.testing.testing_data import (
    RLS_CONFIG_CASES,
    config_to_comparable,
)


def test_group_names_by_account_type():
    account_types = FieldRLSSerializer._group_subject_names_by_type(["@sa:123", "@group:g" "user1", "user2", "@sa:456"])
    assert account_types == FieldRLSSerializer.AccountGroups(["user1", "user2"], ["@sa:123", "@sa:456"], ["@group:g"])


def test_parse_sa_str():
    parsed = FieldRLSSerializer._parse_sa_str("@sa:123")
    assert parsed == RLSSubject(subject_type=RLSSubjectType.user, subject_id="123", subject_name="@sa:123")


def test_parse_group_str():
    parsed = FieldRLSSerializer._parse_group_str("@group:g")
    assert parsed == RLSSubject(subject_type=RLSSubjectType.group, subject_id="g", subject_name="@group:g")


@pytest.mark.parametrize("case", RLS_CONFIG_CASES, ids=[c["name"] for c in RLS_CONFIG_CASES])
def test_rls_entries_to_text_config(case):
    expected_config = case["config_to_compare"]
    rls_entries = case["rls_entries"]

    config = FieldRLSSerializer.to_text_config(rls_entries)

    assert config_to_comparable(config) == config_to_comparable(expected_config)
