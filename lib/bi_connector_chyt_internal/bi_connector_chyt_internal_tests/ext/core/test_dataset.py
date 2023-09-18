from typing import Generic, TypeVar

import pytest

from dl_testing.regulated_test import RegulatedTestParams
from dl_core_testing.testcases.dataset import DefaultDatasetTestSuite
from dl_core_testing.database import DbTable

from bi_connector_chyt_internal.core.constants import (
    SOURCE_TYPE_CHYT_TABLE,
    SOURCE_TYPE_CHYT_TABLE_RANGE,
    SOURCE_TYPE_CHYT_SUBSELECT,
    SOURCE_TYPE_CHYT_USER_AUTH_TABLE,
    SOURCE_TYPE_CHYT_USER_AUTH_TABLE_RANGE,
    SOURCE_TYPE_CHYT_USER_AUTH_SUBSELECT,
)
from bi_connector_chyt_internal.core.us_connection import ConnectionCHYTInternalToken, ConnectionCHYTUserAuth

from bi_connector_chyt_internal_tests.ext.core.base import BaseCHYTTestClass, BaseCHYTUserAuthTestClass
from bi_connector_chyt_internal_tests.ext.core.config import (
    RANGE_DATASET_DSRC_PARAMS, SUBSELECT_DSRC_PARAMS,
)

_CONN_TV = TypeVar('_CONN_TV', ConnectionCHYTInternalToken, ConnectionCHYTUserAuth)


class CHYTDatasetSelectTestClass(DefaultDatasetTestSuite[_CONN_TV], Generic[_CONN_TV]):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultDatasetTestSuite.test_get_param_hash: 'No data source templates',
        },
    )

    @pytest.fixture(scope='function')
    def dsrc_params(self, dataset_table: DbTable) -> dict:
        return dict(
            table_name=dataset_table.name,
        )


class TestCHYTDatasetSelect(BaseCHYTTestClass, CHYTDatasetSelectTestClass[ConnectionCHYTInternalToken]):
    source_type = SOURCE_TYPE_CHYT_TABLE


class TestCHYTUserAuthDatasetSelect(BaseCHYTUserAuthTestClass, CHYTDatasetSelectTestClass[ConnectionCHYTUserAuth]):
    source_type = SOURCE_TYPE_CHYT_USER_AUTH_TABLE


class TestCHYTRangeDatasetSelect(BaseCHYTTestClass, CHYTDatasetSelectTestClass[ConnectionCHYTInternalToken]):
    source_type = SOURCE_TYPE_CHYT_TABLE_RANGE

    @pytest.fixture(scope='function')
    def dsrc_params(self) -> dict:
        return RANGE_DATASET_DSRC_PARAMS


class TestCHYTUserAuthRangeDatasetSelect(BaseCHYTUserAuthTestClass, CHYTDatasetSelectTestClass[ConnectionCHYTUserAuth]):
    source_type = SOURCE_TYPE_CHYT_USER_AUTH_TABLE_RANGE

    @pytest.fixture(scope='function')
    def dsrc_params(self) -> dict:
        return RANGE_DATASET_DSRC_PARAMS


class TestCHYTDatasetSubselect(BaseCHYTTestClass, CHYTDatasetSelectTestClass[ConnectionCHYTInternalToken]):
    source_type = SOURCE_TYPE_CHYT_SUBSELECT

    @pytest.fixture(scope='function')
    def dsrc_params(self) -> dict:
        return SUBSELECT_DSRC_PARAMS


class TestCHYTUserAuthDatasetSubselect(BaseCHYTUserAuthTestClass, CHYTDatasetSelectTestClass[ConnectionCHYTUserAuth]):
    source_type = SOURCE_TYPE_CHYT_USER_AUTH_SUBSELECT

    @pytest.fixture(scope='function')
    def dsrc_params(self) -> dict:
        return SUBSELECT_DSRC_PARAMS
