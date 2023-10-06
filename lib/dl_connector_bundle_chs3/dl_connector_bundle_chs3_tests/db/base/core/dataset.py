import abc
from typing import Generic

import pytest

from dl_core.services_registry import ServicesRegistry
from dl_core.us_dataset import Dataset
from dl_core_testing.database import DbTable
from dl_core_testing.dataset_wrappers import DatasetTestWrapper
from dl_core_testing.testcases.dataset import DefaultDatasetTestSuite

from dl_connector_bundle_chs3.chs3_base.core.us_connection import BaseFileS3Connection
from dl_connector_bundle_chs3.file.core.us_connection import FileS3Connection
from dl_connector_bundle_chs3_tests.db.base.core.base import (
    FILE_CONN_TV,
    BaseCHS3TestClass,
)


class CHS3DatasetTestBase(BaseCHS3TestClass, DefaultDatasetTestSuite, Generic[FILE_CONN_TV], abc.ABC):
    @pytest.fixture(scope="function")
    def dsrc_params(self, dataset_table: DbTable, sample_file_data_source: BaseFileS3Connection.FileDataSource) -> dict:
        return dict(
            origin_source_id=sample_file_data_source.id,
        )

    def test_get_param_hash(
        self,
        sample_table: DbTable,
        saved_connection: FileS3Connection,
        saved_dataset: Dataset,
        conn_default_service_registry: ServicesRegistry,
        dataset_wrapper: DatasetTestWrapper,
        sample_file_data_source: BaseFileS3Connection.FileDataSource,
    ) -> None:
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
            if template.parameters["origin_source_id"] == sample_file_data_source.id:
                found_template = True
                hash_from_template = template.get_param_hash()
                assert hash_from_dataset == hash_from_template

        assert found_template
