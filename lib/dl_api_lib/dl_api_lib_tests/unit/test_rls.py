import pytest

from dl_api_lib.utils.rls import FieldRLSSerializer
from dl_api_lib_testing.rls import (
    RLS_CONFIG_CASES,
    config_to_comparable,
)
from dl_constants.enums import RLSSubjectType
from dl_core.rls import RLSSubject


def test_group_names_by_account_type():
    account_types = FieldRLSSerializer._group_names_by_account_type(["@sa:123", "user1", "user2", "@sa:456"])
    assert account_types == FieldRLSSerializer.AccountGroups(["user1", "user2"], ["@sa:123", "@sa:456"])


def test_parse_sa_str():
    parsed = FieldRLSSerializer._parse_sa_account_str("@sa:123")
    assert parsed == RLSSubject(subject_type=RLSSubjectType.user, subject_id="123", subject_name="@sa:123")


@pytest.mark.parametrize("case", RLS_CONFIG_CASES, ids=[c["name"] for c in RLS_CONFIG_CASES])
def test_rls_entries_to_text_config(case):
    expected_config = case["config_to_compare"]
    rls_entries = case["rls_entries"]

    config = FieldRLSSerializer.to_text_config(rls_entries)

    assert config_to_comparable(config) == config_to_comparable(expected_config)
