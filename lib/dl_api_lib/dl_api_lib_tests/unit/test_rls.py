import pytest

from dl_api_lib.utils.rls import FieldRLSSerializer
from dl_api_lib_testing.rls import (
    RLS_CONFIG_CASES,
    config_to_comparable,
)


@pytest.mark.parametrize("case", RLS_CONFIG_CASES, ids=[c["name"] for c in RLS_CONFIG_CASES])
def test_rls_entries_to_text_config(case):
    expected_config = case["config_to_compare"]
    rls_entries = case["rls_entries"]

    config = FieldRLSSerializer.to_text_config(rls_entries)

    assert config_to_comparable(config) == config_to_comparable(expected_config)
