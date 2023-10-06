import abc
from typing import TypeVar

import pytest

from dl_constants.enums import UserDataType
from dl_core.db import SchemaColumn
from dl_core.db.native_type import GenericNativeType
from dl_core_testing.fixtures.sample_tables import TABLE_SPEC_SAMPLE_SUPERSTORE
from dl_core_testing.testcases.data_source import DefaultDataSourceTestClass

from dl_connector_bundle_chs3.chs3_base.core.data_source import BaseFileS3DataSource
from dl_connector_bundle_chs3.chs3_base.core.data_source_spec import BaseFileS3DataSourceSpec
from dl_connector_bundle_chs3.chs3_base.core.us_connection import BaseFileS3Connection
from dl_connector_bundle_chs3.file.core.constants import SOURCE_TYPE_FILE_S3_TABLE
from dl_connector_bundle_chs3_tests.db.base.core.base import (
    FILE_CONN_TV,
    BaseCHS3TestClass,
)


FILE_DSRC_SPEC_TV = TypeVar("FILE_DSRC_SPEC_TV", bound=BaseFileS3DataSourceSpec)
FILE_DSRC_TV = TypeVar("FILE_DSRC_TV", bound=BaseFileS3DataSource)


class CHS3TableDataSourceTestBase(
    BaseCHS3TestClass,
    DefaultDataSourceTestClass[FILE_CONN_TV, FILE_DSRC_SPEC_TV, FILE_DSRC_TV],
    metaclass=abc.ABCMeta,
):
    @pytest.fixture(scope="function")
    def initial_data_source_spec(
        self,
        sample_file_data_source: BaseFileS3Connection.FileDataSource,
    ) -> FILE_DSRC_SPEC_TV:
        dsrc_spec = BaseFileS3DataSourceSpec(
            source_type=SOURCE_TYPE_FILE_S3_TABLE,
            raw_schema=sample_file_data_source.raw_schema,
            s3_endpoint=self.connection_settings.S3_ENDPOINT,
            bucket=self.connection_settings.BUCKET,
            origin_source_id=sample_file_data_source.id,
        )
        return dsrc_spec

    def get_expected_simplified_schema(self) -> list[tuple[str, UserDataType]]:
        return list(TABLE_SPEC_SAMPLE_SUPERSTORE.table_schema)

    def test_build_from_clause(
        self,
        data_source: FILE_DSRC_TV,
        sample_file_data_source: BaseFileS3Connection.FileDataSource,
        saved_connection: FILE_CONN_TV,
    ) -> None:
        data_source._spec.raw_schema = [  # leaving one column to simplify the test
            SchemaColumn(
                name="c1",
                native_type=GenericNativeType(conn_type=self.conn_type, name="Int64"),
                user_type=UserDataType.integer,
            ),
        ]
        query_from = data_source.get_sql_source().compile(compile_kwargs={"literal_binds": True}).string

        replace_secret = saved_connection.get_conn_dto().replace_secret

        expected = (
            f"s3("
            f"'{self.connection_settings.S3_ENDPOINT}/{self.connection_settings.BUCKET}/"
            f"{sample_file_data_source.s3_filename}', "
            f"'key_id_{replace_secret}', 'secret_key_{replace_secret}', 'Native', "
            f"'c1 Nullable(Int64)')"
        )
        assert query_from == expected, query_from
