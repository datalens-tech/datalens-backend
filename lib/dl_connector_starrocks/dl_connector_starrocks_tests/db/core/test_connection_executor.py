from typing import Sequence

import pytest
from sqlalchemy.dialects import mysql as mysql_types

from dl_constants.enums import UserDataType
from dl_core_testing.testcases.connection_executor import (
    DefaultAsyncConnectionExecutorTestSuite,
    DefaultSyncConnectionExecutorTestSuite,
)
from dl_core.connection_models.common_models import DBIdent
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_starrocks_tests.db.core.base import BaseStarRocksTestClass
import dl_connector_starrocks_tests.db.config as test_config


class TestStarRocksSyncConnectionExecutor(
    BaseStarRocksTestClass,
    DefaultSyncConnectionExecutorTestSuite,
):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultSyncConnectionExecutorTestSuite.test_type_recognition: (
                "StarRocks requires DUPLICATE KEY clause in CREATE TABLE which is not "
                "supported by the generic test framework DDL generation"
            ),
        },
    )

    @pytest.fixture(scope="function")
    def db_ident(self) -> DBIdent:
        return DBIdent(db_name=test_config.CoreConnectionSettings.DB_NAME)

    def get_schemas_for_type_recognition(self) -> dict[str, Sequence[DefaultSyncConnectionExecutorTestSuite.CD]]:
        """Override to use MySQL-specific types that match StarRocks native type names"""
        return {
            "starrocks_types_number": [
                self.CD(mysql_types.INTEGER(), UserDataType.integer),
                self.CD(mysql_types.BIGINT(), UserDataType.integer),
                self.CD(mysql_types.FLOAT(), UserDataType.float),
                self.CD(mysql_types.DOUBLE(), UserDataType.float),
            ],
            "starrocks_types_string": [
                self.CD(mysql_types.VARCHAR(100), UserDataType.string),
                # StarRocks converts TEXT to VARCHAR(65533) internally
                self.CD(mysql_types.TEXT(), UserDataType.string, nt_name="varchar"),
            ],
            "starrocks_types_date": [
                self.CD(mysql_types.DATE(), UserDataType.date),
                self.CD(mysql_types.DATETIME(), UserDataType.genericdatetime),
            ],
            "starrocks_types_other": [
                self.CD(mysql_types.BOOLEAN(), UserDataType.boolean),
                # Add a second column to avoid StarRocks single-column table restriction
                self.CD(mysql_types.INTEGER(), UserDataType.integer),
            ],
        }


class TestStarRocksAsyncConnectionExecutor(
    BaseStarRocksTestClass,
    DefaultAsyncConnectionExecutorTestSuite,
):
    @pytest.fixture(scope="function")
    def db_ident(self) -> DBIdent:
        return DBIdent(db_name=test_config.CoreConnectionSettings.DB_NAME)
