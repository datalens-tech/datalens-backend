from collections.abc import Iterable

import pytest

from dl_api_client.dsmaker.api.data_api import (
    HttpDataApiResponse,
    SyncHttpDataApiV2,
)
from dl_api_client.dsmaker.primitives import (
    Dataset,
    WhereClause,
)
from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_api_lib_testing.connection_base import ConnectionTestBase
from dl_api_lib_testing.data_api_base import (
    DataApiTestParams,
    StandardizedDataApiTestBase,
)
from dl_api_lib_testing.dataset_base import DatasetTestBase
from dl_constants import WhereClauseOperation
from dl_sqlalchemy_metrica_api.api_info.appmetrica import AppMetricaFieldsNamespaces
from dl_sqlalchemy_metrica_api.api_info.metrika import MetrikaApiCounterSource

from dl_connector_metrica.core.constants import (
    CONNECTION_TYPE_APPMETRICA_API,
    CONNECTION_TYPE_METRICA_API,
    SOURCE_TYPE_APPMETRICA_API,
    SOURCE_TYPE_METRICA_API,
)
from dl_connector_metrica_tests.ext.config import (
    API_TEST_CONFIG,
    APPMETRICA_SAMPLE_COUNTER_ID,
    METRIKA_SAMPLE_COUNTER_ID,
)
from dl_connector_metrica_tests.ext.core.base import (
    BaseAppMetricaTestClass,
    BaseMetricaTestClass,
)

# The shared sample counter (44147844) does not always have data in the rolling
# 60-day window that the Metrika dialect injects when no date filter is given.
# Pin a known-populated week so the data API tests stay deterministic.
PINNED_DATE_RANGE = ["2023-12-01", "2023-12-07 12:00:00"]


class MetricaConnectionTestBase(BaseMetricaTestClass, ConnectionTestBase):
    conn_type = CONNECTION_TYPE_METRICA_API
    compeng_enabled = False

    @pytest.fixture(scope="class")
    def bi_test_config(self) -> ApiTestEnvironmentConfiguration:
        return API_TEST_CONFIG

    @pytest.fixture(scope="class")
    def connection_params(self, metrica_token: str) -> dict:
        return {
            "counter_id": METRIKA_SAMPLE_COUNTER_ID,
            "token": metrica_token,
            "accuracy": 0.01,
        }


class MetricaDatasetTestBase(MetricaConnectionTestBase, DatasetTestBase):
    @pytest.fixture(scope="class")
    def dataset_params(self) -> dict:
        return {
            "source_type": SOURCE_TYPE_METRICA_API.name,
            "parameters": {
                "db_name": MetrikaApiCounterSource.hits.name,
            },
        }


class MetricaDataApiTestBase(MetricaDatasetTestBase, StandardizedDataApiTestBase):
    mutation_caches_enabled = False

    @pytest.fixture(scope="class")
    def data_api_test_params(self) -> DataApiTestParams:
        return DataApiTestParams(
            two_dims=("Домен страницы", "Просмотры"),
            summable_field="Просмотры",
            range_field="Дата и время просмотра",
            distinct_field="Область",
            date_field="Дата просмотра",
        )

    def pinned_date_filter(self, ds: Dataset) -> WhereClause:
        return ds.find_field(title="Дата просмотра").filter(WhereClauseOperation.BETWEEN, values=PINNED_DATE_RANGE)

    def get_result(
        self,
        ds: Dataset,
        data_api: SyncHttpDataApiV2,
        field_names: Iterable[str],
        filters: list[WhereClause] | None = None,
        query_params: dict | None = None,
        fail_ok: bool = False,
    ) -> HttpDataApiResponse:
        return super().get_result(
            ds,
            data_api,
            field_names,
            filters=[self.pinned_date_filter(ds), *(filters or [])],
            query_params=query_params,
            fail_ok=fail_ok,
        )

    def get_distinct(
        self,
        ds: Dataset,
        data_api: SyncHttpDataApiV2,
        field_name: str,
        filters: list[WhereClause] | None = None,
        ignore_nonexistent_filters: bool | None = None,
    ) -> HttpDataApiResponse:
        return super().get_distinct(
            ds,
            data_api,
            field_name,
            filters=[self.pinned_date_filter(ds), *(filters or [])],
            ignore_nonexistent_filters=ignore_nonexistent_filters,
        )

    def get_result_ordered(
        self,
        ds: Dataset,
        data_api: SyncHttpDataApiV2,
        field_names: Iterable[str],
        order_by: Iterable[str],
        filters: list[WhereClause] | None = None,
    ) -> HttpDataApiResponse:
        return super().get_result_ordered(
            ds,
            data_api,
            field_names,
            order_by,
            filters=[self.pinned_date_filter(ds), *(filters or [])],
        )


class AppMetricaConnectionTestBase(BaseAppMetricaTestClass, ConnectionTestBase):
    conn_type = CONNECTION_TYPE_APPMETRICA_API
    compeng_enabled = False

    @pytest.fixture(scope="class")
    def bi_test_config(self) -> ApiTestEnvironmentConfiguration:
        return API_TEST_CONFIG

    @pytest.fixture(scope="class")
    def connection_params(self, metrica_token: str) -> dict:
        return {
            "counter_id": APPMETRICA_SAMPLE_COUNTER_ID,
            "token": metrica_token,
            "accuracy": 0.01,
        }


class AppMetricaDatasetTestBase(AppMetricaConnectionTestBase, DatasetTestBase):
    @pytest.fixture(scope="class")
    def dataset_params(self) -> dict:
        return {
            "source_type": SOURCE_TYPE_APPMETRICA_API.name,
            "parameters": {
                "db_name": AppMetricaFieldsNamespaces.installs.name,
            },
        }
