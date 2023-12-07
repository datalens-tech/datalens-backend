import pytest

from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_api_lib_testing.connection_base import ConnectionTestBase
from dl_api_lib_testing.data_api_base import (
    DataApiTestParams,
    StandardizedDataApiTestBase,
)
from dl_api_lib_testing.dataset_base import DatasetTestBase
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


class MetricaConnectionTestBase(BaseMetricaTestClass, ConnectionTestBase):
    conn_type = CONNECTION_TYPE_METRICA_API
    compeng_enabled = False

    @pytest.fixture(scope="class")
    def bi_test_config(self) -> ApiTestEnvironmentConfiguration:
        return API_TEST_CONFIG

    @pytest.fixture(scope="class")
    def connection_params(self, metrica_token: str) -> dict:
        return dict(
            counter_id=METRIKA_SAMPLE_COUNTER_ID,
            token=metrica_token,
            accuracy=0.01,
        )


class MetricaDatasetTestBase(MetricaConnectionTestBase, DatasetTestBase):
    @pytest.fixture(scope="class")
    def dataset_params(self) -> dict:
        return dict(
            source_type=SOURCE_TYPE_METRICA_API.name,
            parameters=dict(
                db_name=MetrikaApiCounterSource.hits.name,
            ),
        )


class MetricaDataApiTestBase(MetricaDatasetTestBase, StandardizedDataApiTestBase):
    mutation_caches_enabled = False

    @pytest.fixture(scope="class")
    def data_api_test_params(self) -> DataApiTestParams:
        return DataApiTestParams(
            two_dims=("Домен страницы", "Просмотров в минуту"),
            summable_field="Просмотров в минуту",
            range_field="Дата и время просмотра",
            distinct_field="Адрес страницы",
            date_field="Дата просмотра",
        )


class AppMetricaConnectionTestBase(BaseAppMetricaTestClass, ConnectionTestBase):
    conn_type = CONNECTION_TYPE_APPMETRICA_API
    compeng_enabled = False

    @pytest.fixture(scope="class")
    def bi_test_config(self) -> ApiTestEnvironmentConfiguration:
        return API_TEST_CONFIG

    @pytest.fixture(scope="class")
    def connection_params(self, metrica_token: str) -> dict:
        return dict(
            counter_id=APPMETRICA_SAMPLE_COUNTER_ID,
            token=metrica_token,
            accuracy=0.01,
        )


class AppMetricaDatasetTestBase(AppMetricaConnectionTestBase, DatasetTestBase):
    @pytest.fixture(scope="class")
    def dataset_params(self) -> dict:
        return dict(
            source_type=SOURCE_TYPE_APPMETRICA_API.name,
            parameters=dict(
                db_name=AppMetricaFieldsNamespaces.installs.name,
            ),
        )
