import pytest

from dl_rls.rls import RLS
from dl_rls.schema import RLSSchema
from dl_rls.testing.testing_data import RLS_CONFIG_CASES


@pytest.mark.parametrize("case", RLS_CONFIG_CASES, ids=[c["name"] for c in RLS_CONFIG_CASES])
def test_rls_schema(case):
    rls = RLS(items=case["rls_entries"])
    schema = RLSSchema()

    dumped = schema.dump(rls)
    assert isinstance(dumped, list)
    assert len(dumped) == len(case["rls_entries"])
    loaded = schema.load(dumped)
    assert loaded == rls
