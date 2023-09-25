import pytest

from dl_constants.enums import BIType

from dl_connector_postgresql.core.postgresql_base.type_transformer import PostgreSQLTypeTransformer


@pytest.mark.parametrize("array_type", (BIType.array_int, BIType.array_float, BIType.array_str))
def test_null_array_conversion(array_type):
    tt = PostgreSQLTypeTransformer
    assert tt.cast_for_output(None, user_t=array_type) is None
