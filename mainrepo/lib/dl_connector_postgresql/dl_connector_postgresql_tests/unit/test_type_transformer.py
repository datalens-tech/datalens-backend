import pytest

from dl_connector_postgresql.core.postgresql_base.type_transformer import PostgreSQLTypeTransformer
from dl_constants.enums import UserDataType


@pytest.mark.parametrize("array_type", (UserDataType.array_int, UserDataType.array_float, UserDataType.array_str))
def test_null_array_conversion(array_type):
    tt = PostgreSQLTypeTransformer
    assert tt.cast_for_output(None, user_t=array_type) is None
