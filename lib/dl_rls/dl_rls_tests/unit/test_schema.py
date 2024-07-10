import pytest

from dl_rls.rls import RLS
from dl_rls.schema import RLSSchema
from dl_rls.testing.testing_data import RLS_CONFIG_CASES


@pytest.mark.parametrize("case", RLS_CONFIG_CASES, ids=[c["name"] for c in RLS_CONFIG_CASES])
@pytest.mark.parametrize("groups", [set(), {"group1", "group2"}], ids=["no groups", "some groups"])
def test_rls_schema(case, groups):
    rls = RLS(items=case["rls_entries"])
    rls.allowed_groups = groups
    schema = RLSSchema()

    dumped = schema.dump(rls)
    assert isinstance(dumped, list)
    assert len(dumped) == len(case["rls_entries"])

    loaded = schema.load(dumped)
    assert not loaded.allowed_groups
    assert loaded.items == rls.items
