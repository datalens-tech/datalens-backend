from __future__ import annotations

import pytest

from dl_constants.enums import DataSourceRole
from dl_core.us_dataset import Dataset
from dl_core_testing.dataset_wrappers import DatasetTestWrapper

# To use fixture "loop" in each test in module
#  Cause: if any other test before will use it default loop for thread will be set to None
pytestmark = pytest.mark.usefixtures("loop")


def _check_source_exists(dataset, sync_usm, service_registry):
    dataset = sync_usm.get_by_id(dataset.uuid, expected_type=Dataset)
    ds_wrapper = DatasetTestWrapper(dataset=dataset, us_manager=sync_usm)
    dsrc = ds_wrapper.get_data_source_strict(source_id=dataset.get_single_data_source_id(), role=DataSourceRole.origin)
    conn_executor = service_registry.get_conn_executor_factory().get_sync_conn_executor(conn=dsrc.connection)
    assert dsrc.source_exists(conn_executor_factory=lambda: conn_executor)


def test_source_exists_view(default_sync_usm, saved_dataset_for_view, default_service_registry):
    _check_source_exists(
        saved_dataset_for_view,
        sync_usm=default_sync_usm,
        service_registry=default_service_registry,
    )


def test_source_exists_view_with_schema(
    default_sync_usm,
    saved_schematized_dataset_for_view,
    default_service_registry,
):
    _check_source_exists(
        saved_schematized_dataset_for_view,
        sync_usm=default_sync_usm,
        service_registry=default_service_registry,
    )
