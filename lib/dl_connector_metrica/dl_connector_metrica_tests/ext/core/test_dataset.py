from typing import Optional

import pytest
import sqlalchemy as sa

from dl_constants.enums import (
    DataSourceRole,
    UserDataType,
)
from dl_core.dataset_capabilities import DatasetCapabilities
from dl_core.query.bi_query import BIQuery
from dl_core.query.expression import (
    ExpressionCtx,
    OrderByExpressionCtx,
)
from dl_core.services_registry.top_level import ServicesRegistry
from dl_core.us_dataset import Dataset
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core_testing.connection import make_saved_connection
from dl_core_testing.connector import (
    CONNECTION_TYPE_TESTING,
    SOURCE_TYPE_TESTING,
)
from dl_core_testing.dataset_wrappers import DatasetTestWrapper
from dl_core_testing.testcases.dataset import DefaultDatasetTestSuite
from dl_sqlalchemy_metrica_api.api_info.appmetrica import AppMetricaFieldsNamespaces
from dl_sqlalchemy_metrica_api.api_info.metrika import MetrikaApiCounterSource
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_metrica.core.constants import (
    SOURCE_TYPE_APPMETRICA_API,
    SOURCE_TYPE_METRICA_API,
)
from dl_connector_metrica.core.us_connection import (
    AppMetricaApiConnection,
    MetrikaApiConnection,
)
from dl_connector_metrica_tests.ext.core.base import (
    BaseAppMetricaTestClass,
    BaseMetricaTestClass,
)


class TestMetricaDataset(BaseMetricaTestClass, DefaultDatasetTestSuite[MetrikaApiConnection]):
    source_type = SOURCE_TYPE_METRICA_API

    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultDatasetTestSuite.test_simple_select_from_subquery: "Not allowed for Metrica",
        },
        mark_tests_failed={
            DefaultDatasetTestSuite.test_get_param_hash: "",  # TODO: FIXME
        },
    )

    @pytest.fixture(scope="function")
    def dsrc_params(self) -> dict:
        return dict(
            db_name=MetrikaApiCounterSource.hits.name,
        )

    def _check_simple_select(
        self,
        dataset_wrapper: DatasetTestWrapper,
        saved_dataset: Dataset,
        async_service_registry: ServicesRegistry,
        sync_us_manager: SyncUSManager,
        result_cnt: int,
        limit: Optional[int] = None,
        from_subquery: bool = False,
        subquery_limit: Optional[int] = None,
    ) -> None:
        assert limit is not None or (from_subquery and subquery_limit is not None)
        avatar_id = dataset_wrapper.get_root_avatar_strict().id
        bi_query = BIQuery(
            select_expressions=[
                ExpressionCtx(
                    expression=sa.literal_column(
                        dataset_wrapper.quote("ym:pv:startOfHour", role=DataSourceRole.origin)
                    ),
                    avatar_ids=[avatar_id],
                    alias="col1",
                    user_type=UserDataType.datetime,
                ),  # is a dimension
                ExpressionCtx(
                    expression=sa.literal_column(
                        dataset_wrapper.quote("ym:pv:pageviewsPerMinute", role=DataSourceRole.origin)
                    ),
                    avatar_ids=[avatar_id],
                    alias="col2",
                    user_type=UserDataType.integer,
                ),  # is a measure
            ],
            group_by_expressions=[
                ExpressionCtx(
                    expression=sa.literal_column(
                        dataset_wrapper.quote("ym:pv:startOfHour", role=DataSourceRole.origin)
                    ),
                    avatar_ids=[avatar_id],
                    alias="col1",
                    user_type=UserDataType.datetime,
                ),
            ],
            limit=limit,
        )

        data = self.fetch_data(
            saved_dataset=saved_dataset,
            service_registry=async_service_registry,
            sync_us_manager=sync_us_manager,
            bi_query=bi_query,
            from_subquery=from_subquery,
            subquery_limit=subquery_limit,
        )
        assert len(list(data.data)) == result_cnt

    def test_select_data_distinct(
        self,
        dataset_wrapper: DatasetTestWrapper,
        saved_dataset: Dataset,
        conn_async_service_registry: ServicesRegistry,
        sync_us_manager: SyncUSManager,
    ) -> None:
        avatar_id = dataset_wrapper.get_root_avatar_strict().id
        bi_query = BIQuery(
            select_expressions=[
                ExpressionCtx(
                    expression=sa.literal_column(
                        dataset_wrapper.quote("ym:pv:startOfHour", role=DataSourceRole.origin)
                    ),
                    avatar_ids=[avatar_id],
                    alias="col1",
                    user_type=UserDataType.datetime,
                ),  # is a dimension
            ],
            order_by_expressions=[
                OrderByExpressionCtx(
                    expression=sa.literal_column(
                        dataset_wrapper.quote("ym:pv:startOfHour", role=DataSourceRole.origin)
                    ),
                    avatar_ids=[avatar_id],
                    alias="col1",
                    user_type=UserDataType.datetime,
                ),
            ],
            distinct=True,
        )

        data = self.fetch_data(
            saved_dataset=saved_dataset,
            service_registry=conn_async_service_registry,
            sync_us_manager=sync_us_manager,
            bi_query=bi_query,
        )
        values = [row[0] for row in data.data]
        assert values == sorted(set(values))

    def test_select_with_quotes(
        self,
        dataset_wrapper: DatasetTestWrapper,
        saved_dataset: Dataset,
        conn_async_service_registry: ServicesRegistry,
        sync_us_manager: SyncUSManager,
    ) -> None:
        avatar_id = dataset_wrapper.get_root_avatar_strict().id
        bi_query = BIQuery(
            select_expressions=[
                ExpressionCtx(
                    expression=sa.literal_column(
                        dataset_wrapper.quote("ym:pv:startOfHour", role=DataSourceRole.origin)
                    ),
                    avatar_ids=[avatar_id],
                    alias="col1",
                    user_type=UserDataType.datetime,
                ),  # is a dimension
                ExpressionCtx(
                    expression=sa.literal_column(
                        dataset_wrapper.quote("ym:pv:pageviewsPerMinute", role=DataSourceRole.origin)
                    ),
                    avatar_ids=[avatar_id],
                    alias="col2",
                    user_type=UserDataType.integer,
                ),  # is a measure
            ],
            group_by_expressions=[
                ExpressionCtx(
                    expression=sa.literal_column(
                        dataset_wrapper.quote("ym:pv:startOfHour", role=DataSourceRole.origin)
                    ),
                    avatar_ids=[avatar_id],
                    alias="col1",
                    user_type=UserDataType.datetime,
                ),
            ],
            dimension_filters=[
                ExpressionCtx(
                    expression=sa.literal_column("ym:pv:openstatCampaign").in_(
                        ("Nizhny Novgorod Oblast'", "'m'a'n'y'q'u'o't'e's'")
                    ),
                    avatar_ids=[avatar_id],
                    user_type=UserDataType.boolean,
                ),
            ],
        )

        data = self.fetch_data(
            saved_dataset=saved_dataset,
            service_registry=conn_async_service_registry,
            sync_us_manager=sync_us_manager,
            bi_query=bi_query,
        )
        assert not list(data.data)  # not expecting any data, just checking that the request was successful

    def test_source_cannot_be_added(
        self,
        dataset_wrapper: DatasetTestWrapper,
        saved_dataset: Dataset,
        saved_connection: MetrikaApiConnection,
        sync_us_manager: SyncUSManager,
    ) -> None:
        testing_conn = make_saved_connection(sync_us_manager, conn_type=CONNECTION_TYPE_TESTING)
        try:
            capabilities = DatasetCapabilities(
                dataset=saved_dataset, dsrc_coll_factory=dataset_wrapper.dsrc_coll_factory
            )
            assert not capabilities.source_can_be_added(
                connection_id=saved_connection.uuid, created_from=SOURCE_TYPE_METRICA_API
            )
            assert not capabilities.source_can_be_added(
                connection_id=testing_conn.uuid, created_from=SOURCE_TYPE_TESTING
            )
        finally:
            sync_us_manager.delete(testing_conn)


class TestAppMetricaDataset(BaseAppMetricaTestClass, DefaultDatasetTestSuite[AppMetricaApiConnection]):
    source_type = SOURCE_TYPE_APPMETRICA_API

    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultDatasetTestSuite.test_simple_select_from_subquery: "Not allowed for AppMetrica",
        },
        mark_tests_failed={
            DefaultDatasetTestSuite.test_get_param_hash: "",  # TODO: FIXME
        },
    )

    @pytest.fixture(scope="function")
    def dsrc_params(self) -> dict:
        return dict(
            db_name=AppMetricaFieldsNamespaces.installs.name,
        )

    def _check_simple_select(
        self,
        dataset_wrapper: DatasetTestWrapper,
        saved_dataset: Dataset,
        async_service_registry: ServicesRegistry,
        sync_us_manager: SyncUSManager,
        result_cnt: int,
        limit: Optional[int] = None,
        from_subquery: bool = False,
        subquery_limit: Optional[int] = None,
    ) -> None:
        assert limit is not None or (from_subquery and subquery_limit is not None)
        avatar_id = dataset_wrapper.get_root_avatar_strict().id
        bi_query = BIQuery(
            select_expressions=[
                ExpressionCtx(
                    expression=sa.literal_column(
                        dataset_wrapper.quote("ym:ts:regionCityName", role=DataSourceRole.origin)
                    ),
                    avatar_ids=[avatar_id],
                    alias="col1",
                    user_type=UserDataType.string,
                ),  # is a dimension
                ExpressionCtx(
                    expression=sa.literal_column(
                        dataset_wrapper.quote("ym:ts:advInstallDevices", role=DataSourceRole.origin)
                    ),
                    avatar_ids=[avatar_id],
                    alias="col2",
                    user_type=UserDataType.integer,
                ),  # is a measure
            ],
            group_by_expressions=[
                ExpressionCtx(
                    expression=sa.literal_column(
                        dataset_wrapper.quote("ym:ts:regionCityName", role=DataSourceRole.origin)
                    ),
                    avatar_ids=[avatar_id],
                    alias="col1",
                    user_type=UserDataType.string,
                ),
            ],
            limit=limit,
        )

        data = self.fetch_data(
            saved_dataset=saved_dataset,
            service_registry=async_service_registry,
            sync_us_manager=sync_us_manager,
            bi_query=bi_query,
            from_subquery=from_subquery,
            subquery_limit=subquery_limit,
        )
        assert len(list(data.data)) == result_cnt
