from aiohttp.test_utils import TestClient
import pytest
import shortuuid

from dl_api_lib_testing.connector.dashsql_suite import DefaultDashSQLTestSuite
from dl_constants.enums import UserDataType

from dl_connector_postgresql_tests.db.api.base import PostgreSQLDashSQLConnectionTest


class TestPostgreSQLArrayTypes(PostgreSQLDashSQLConnectionTest, DefaultDashSQLTestSuite):
    @pytest.fixture(scope="function")
    def array_types_table_name(self, db):
        name = f"test_array_types_{shortuuid.uuid().lower()}"

        try:
            query = f"""
                CREATE TABLE {name} (
                    int_small_array SMALLINT[],
                    int_array INTEGER[],
                    int_big_array BIGINT[],

                    float_real_array REAL[],
                    float_double_array DOUBLE PRECISION[],
                    float_numeric_array NUMERIC[],

                    str_char_array CHAR(10)[],
                    str_varchar_array VARCHAR(100)[],
                    str_text_array TEXT[]
                )
            """
            db.execute(query)

            query = f"""
                INSERT INTO {name} VALUES (
                    ARRAY[1, 2, 3]::SMALLINT[],
                    ARRAY[4, 5, 6]::INTEGER[],
                    ARRAY[7, 8, 9]::BIGINT[],

                    ARRAY[1.1, 2.2, 3.3]::REAL[],
                    ARRAY[4.4, 5.5, 6.6]::DOUBLE PRECISION[],
                    ARRAY[7.7, 8.8, 9.9]::NUMERIC[],

                    ARRAY['a', 'b', 'c']::CHAR(10)[],
                    ARRAY['foo', 'bar', 'baz']::VARCHAR(100)[],
                    ARRAY['text1', 'text2', 'text3']::TEXT[]
                )
            """
            db.execute(query)

            yield name

        finally:
            db.execute(f"DROP TABLE IF EXISTS {name}")

    @pytest.mark.asyncio
    async def test_array_type_detection_in_table(
        self,
        data_api_lowlevel_aiohttp_client: TestClient,
        saved_connection_id: str,
        array_types_table_name: str,
    ):
        query = f"SELECT * FROM {array_types_table_name} LIMIT 0"

        resp = await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query=query,
        )

        resp_data = await resp.json()
        assert resp_data[0]["event"] == "metadata", resp_data

        column_names = resp_data[0]["data"]["names"]
        bi_types = resp_data[0]["data"]["bi_types"]

        column_types = dict(zip(column_names, bi_types))

        assert column_types["int_small_array"] == UserDataType.array_int.name
        assert column_types["int_array"] == UserDataType.array_int.name
        assert column_types["int_big_array"] == UserDataType.array_int.name

        assert column_types["float_real_array"] == UserDataType.array_float.name
        assert column_types["float_double_array"] == UserDataType.array_float.name
        assert column_types["float_numeric_array"] == UserDataType.array_float.name

        assert column_types["str_char_array"] == UserDataType.array_str.name
        assert column_types["str_varchar_array"] == UserDataType.array_str.name
        assert column_types["str_text_array"] == UserDataType.array_str.name

    @pytest.mark.asyncio
    async def test_array_type_detection_in_query(
        self,
        data_api_lowlevel_aiohttp_client: TestClient,
        saved_connection_id: str,
    ):
        query = """
            SELECT
                ARRAY[1, 2, 3]::SMALLINT[] as int_small_array,
                ARRAY[4, 5, 6]::INTEGER[] as int_array,
                ARRAY[7, 8, 9]::BIGINT[] as int_big_array,

                ARRAY[1.1, 2.2, 3.3]::REAL[] as float_real_array,
                ARRAY[4.4, 5.5, 6.6]::DOUBLE PRECISION[] as float_double_array,
                ARRAY[7.7, 8.8, 9.9]::NUMERIC[] as float_numeric_array,

                ARRAY['a', 'b', 'c']::CHAR(10)[] as str_char_array,
                ARRAY['foo', 'bar', 'baz']::VARCHAR(100)[] as str_varchar_array,
                ARRAY['text1', 'text2', 'text3']::TEXT[] as str_text_array
        """

        resp = await self.get_dashsql_response(
            data_api_aio=data_api_lowlevel_aiohttp_client,
            conn_id=saved_connection_id,
            query=query,
        )

        resp_data = await resp.json()
        assert resp_data[0]["event"] == "metadata", resp_data

        column_names = resp_data[0]["data"]["names"]
        bi_types = resp_data[0]["data"]["bi_types"]

        column_types = dict(zip(column_names, bi_types))

        assert column_types["int_small_array"] == UserDataType.array_int.name
        assert column_types["int_array"] == UserDataType.array_int.name
        assert column_types["int_big_array"] == UserDataType.array_int.name

        assert column_types["float_real_array"] == UserDataType.array_float.name
        assert column_types["float_double_array"] == UserDataType.array_float.name
        assert column_types["float_numeric_array"] == UserDataType.array_float.name

        assert column_types["str_char_array"] == UserDataType.array_str.name
        assert column_types["str_varchar_array"] == UserDataType.array_str.name
        assert column_types["str_text_array"] == UserDataType.array_str.name
