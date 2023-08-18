from __future__ import annotations

from typing import ClassVar, Generic, Optional, TypeVar

import pytest
import sqlalchemy as sa

from bi_constants.enums import CreateDSFrom, DataSourceRole

from bi_core.data_processing.stream_base import DataStream
from bi_core.query.bi_query import BIQuery
from bi_core.services_registry.top_level import ServicesRegistry
from bi_core.us_connection_base import ConnectionBase
from bi_core.us_dataset import Dataset
from bi_core.us_manager.us_manager_sync import SyncUSManager
from bi_core.query.expression import ExpressionCtx

from bi_core_testing.database import DbTable
from bi_core_testing.testcases.connection import BaseConnectionTestClass
from bi_core_testing.data import DataFetcher
from bi_core_testing.dataset import make_dataset
from bi_core_testing.dataset_wrappers import DatasetTestWrapper, EditableDatasetTestWrapper


_CONN_TV = TypeVar('_CONN_TV', bound=ConnectionBase)


class BaseDatasetTestClass(BaseConnectionTestClass[_CONN_TV], Generic[_CONN_TV]):
    source_type: ClassVar[CreateDSFrom]

    @pytest.fixture(scope='function')
    def dataset_table(self, sample_table: DbTable) -> DbTable:
        """The table to be used for datasets. By default use the sample table"""
        return sample_table

    @pytest.fixture(scope='function')
    def dsrc_params(self, dataset_table: DbTable) -> dict:
        return dict(
            db_name=dataset_table.db.name,
            schema_name=dataset_table.schema,
            table_name=dataset_table.name,
        )

    @pytest.fixture(scope='function')
    def saved_dataset(
            self,
            sync_us_manager: SyncUSManager,
            saved_connection: _CONN_TV, dsrc_params: dict,
    ) -> Dataset:
        dataset = make_dataset(
            sync_usm=sync_us_manager, connection=saved_connection,
            created_from=self.source_type,
            dsrc_params=dsrc_params,
        )
        sync_us_manager.save(dataset)
        return dataset

    @pytest.fixture(scope='function')
    def empty_saved_dataset(self, sync_us_manager: SyncUSManager) -> Dataset:
        dataset = make_dataset(sync_usm=sync_us_manager)
        sync_us_manager.save(dataset)
        return dataset

    @pytest.fixture(scope='function')
    def dataset_wrapper(self, saved_dataset: Dataset, sync_us_manager: SyncUSManager) -> DatasetTestWrapper:
        return DatasetTestWrapper(dataset=saved_dataset, us_manager=sync_us_manager)

    @pytest.fixture(scope='function')
    def editable_dataset_wrapper(
            self, saved_dataset: Dataset, sync_us_manager: SyncUSManager,
    ) -> EditableDatasetTestWrapper:
        return EditableDatasetTestWrapper(dataset=saved_dataset, us_manager=sync_us_manager)

    def fetch_data(
            self,
            saved_dataset: Dataset,
            service_registry: ServicesRegistry,
            sync_us_manager: SyncUSManager,
            bi_query: BIQuery,
            from_subquery: bool = False,
            subquery_limit: Optional[int] = None,
    ) -> DataStream:
        data_fetcher = DataFetcher(
            service_registry=service_registry,
            dataset=saved_dataset, us_manager=sync_us_manager,
        )
        return data_fetcher.get_data_stream(
            role=DataSourceRole.origin,
            bi_query=bi_query,
            from_subquery=from_subquery,
            subquery_limit=subquery_limit,
        )


class DefaultDatasetTestSuite(BaseDatasetTestClass[_CONN_TV], Generic[_CONN_TV]):
    do_check_simple_select: ClassVar[bool] = True
    do_check_param_hash: ClassVar[bool] = True

    def _check_simple_select(
            self,
            dataset_wrapper: DatasetTestWrapper,
            saved_dataset: Dataset,
            async_service_registry: ServicesRegistry,
            sync_us_manager: SyncUSManager,
            result_cnt: int,
            limit: Optional[int] = None,
            from_subquery: bool = False, subquery_limit: Optional[int] = None,
    ) -> None:
        assert limit is not None or (from_subquery and subquery_limit is not None)
        avatar_id = dataset_wrapper.get_root_avatar_strict().id
        raw_schema = dataset_wrapper.get_cached_raw_schema(role=DataSourceRole.origin)
        col_expr = sa.literal_column(dataset_wrapper.quote(raw_schema[0].name, role=DataSourceRole.origin))
        bi_query = BIQuery(
            select_expressions=[
                ExpressionCtx(
                    expression=col_expr,
                    user_type=raw_schema[0].user_type,
                    avatar_ids=[avatar_id],
                    alias=raw_schema[0].name,
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

    def test_simple_select(
            self,
            dataset_wrapper: DatasetTestWrapper,
            saved_dataset: Dataset,
            conn_async_service_registry: ServicesRegistry,
            sync_us_manager: SyncUSManager,
    ) -> None:
        if not self.do_check_simple_select:
            pytest.skip()

        self._check_simple_select(
            dataset_wrapper=dataset_wrapper, saved_dataset=saved_dataset,
            async_service_registry=conn_async_service_registry, sync_us_manager=sync_us_manager,
            limit=5, result_cnt=5,
        )

    def test_simple_select_from_subquery(
            self,
            dataset_wrapper: DatasetTestWrapper,
            saved_dataset: Dataset,
            conn_async_service_registry: ServicesRegistry,
            sync_us_manager: SyncUSManager,
    ) -> None:
        if not self.do_check_simple_select:
            pytest.skip()

        self._check_simple_select(
            dataset_wrapper=dataset_wrapper, saved_dataset=saved_dataset,
            async_service_registry=conn_async_service_registry, sync_us_manager=sync_us_manager,
            from_subquery=True, subquery_limit=5, result_cnt=5,
        )

    def test_get_param_hash(
            self, sample_table: DbTable, saved_connection: ConnectionBase, saved_dataset: Dataset,
            conn_default_service_registry: ServicesRegistry, dataset_wrapper: DatasetTestWrapper,
    ) -> None:
        if not self.do_check_param_hash:
            pytest.skip()

        dataset = saved_dataset
        service_registry = conn_default_service_registry
        source_id = dataset.get_single_data_source_id()
        dsrc_coll = dataset_wrapper.get_data_source_coll_strict(source_id=source_id)
        hash_from_dataset = dsrc_coll.get_param_hash()

        templates = saved_connection.get_data_source_templates(
            conn_executor_factory=service_registry.get_conn_executor_factory().get_sync_conn_executor,
        )
        found_template = False
        for template in templates:
            if template.parameters['table_name'] == sample_table.name:
                found_template = True
                hash_from_template = template.get_param_hash()
                assert hash_from_dataset == hash_from_template

        assert found_template
