import datetime

import pytest
from sqlalchemy_metrika_api.api_info.metrika import MetrikaApiCounterSource

from dl_core.data_source_spec.sql import StandardSQLDataSourceSpec

from bi_connector_metrica.core.constants import SOURCE_TYPE_METRICA_API
from bi_connector_metrica.core.data_source import MetrikaApiDataSource
from bi_connector_metrica.core.us_connection import MetrikaApiConnection

from dl_core_testing.testcases.data_source import BaseDataSourceTestClass
from bi_connector_metrica_tests.ext.core.base import BaseMetricaTestClass


class TestMetricaDataSource(
    BaseMetricaTestClass,
    BaseDataSourceTestClass[
        MetrikaApiConnection,
        StandardSQLDataSourceSpec,
        MetrikaApiDataSource,
    ]
):
    DSRC_CLS = MetrikaApiDataSource

    @pytest.fixture(scope='class')
    def initial_data_source_spec(self) -> StandardSQLDataSourceSpec:
        return StandardSQLDataSourceSpec(
            source_type=SOURCE_TYPE_METRICA_API,
            db_name=MetrikaApiCounterSource.hits.name,
        )

    def test_expression_value_range(self, data_source: MetrikaApiDataSource, saved_connection: MetrikaApiConnection):
        conn = saved_connection
        dsrc = data_source

        # datetime
        min_value, max_value = dsrc.get_expression_value_range(col_name='ym:pv:dateTime')
        now = datetime.datetime.utcnow()
        assert min_value.date() == conn.data.counter_creation_date
        assert max_value - now < datetime.timedelta(seconds=1)

        # date
        min_value, max_value = dsrc.get_expression_value_range(col_name='ym:pv:startOfQuarter')
        now = datetime.datetime.utcnow()
        assert min_value == conn.data.counter_creation_date
        assert max_value == now.date()
