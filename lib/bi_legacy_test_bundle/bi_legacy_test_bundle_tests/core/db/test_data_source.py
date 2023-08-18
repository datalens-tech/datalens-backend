from __future__ import annotations

import pytest

from bi_constants.enums import DataSourceRole

from bi_core import exc
from bi_core.us_dataset import Dataset
from bi_core_testing.dataset_wrappers import DatasetTestWrapper, EditableDatasetTestWrapper


# To use fixture "loop" in each test in module
#  Cause: if any other test before will use it default loop for thread will be set to None
pytestmark = pytest.mark.usefixtures("loop")


def test_get_param_hash(
        clickhouse_table, saved_ch_connection, saved_ch_dataset, default_sync_usm,
        default_service_registry,
):
    us_manager = default_sync_usm
    dataset = saved_ch_dataset
    ds_wrapper = EditableDatasetTestWrapper(dataset=dataset, us_manager=us_manager)
    source_id = dataset.get_single_data_source_id()
    ds_wrapper.update_data_source(source_id=source_id, db_version='1.2.3.4')
    dsrc_coll = ds_wrapper.get_data_source_coll_strict(source_id=source_id)
    hash_from_dataset = dsrc_coll.get_param_hash()
    hash_from_template = None

    templates = saved_ch_connection.get_data_source_templates(
        conn_executor_factory=default_service_registry.get_conn_executor_factory().get_sync_conn_executor,
    )
    for template in templates:
        if template.parameters['table_name'] == clickhouse_table.name:
            hash_from_template = template.get_param_hash()
    assert hash_from_dataset == hash_from_template


def test_ch_hidden_system_database(saved_ch_connection, default_service_registry):
    templates = saved_ch_connection.get_data_source_templates(
        conn_executor_factory=default_service_registry.get_conn_executor_factory().get_sync_conn_executor,
    )
    assert all(t.group[0] != 'system' for t in templates)


def test_get_schema(default_sync_usm, db_table, saved_dataset, app_request_context, default_service_registry):
    sr = default_service_registry
    us_manager = default_sync_usm
    dataset = us_manager.get_by_id(saved_dataset.uuid, expected_type=Dataset)
    ds_wrapper = DatasetTestWrapper(dataset=dataset, us_manager=us_manager)
    source_id = dataset.get_single_data_source_id()
    dsrc = ds_wrapper.get_data_source_strict(source_id=source_id, role=DataSourceRole.origin)
    conn_executor = sr.get_conn_executor_factory().get_sync_conn_executor(conn=dsrc.connection)
    schema = dsrc.get_schema_info(conn_executor_factory=lambda: conn_executor).schema
    assert schema


def test_get_schema_error(default_sync_usm, db_table, saved_dataset, app_request_context, default_service_registry):
    sr = default_service_registry
    us_manager = default_sync_usm
    dataset = saved_dataset
    dataset = us_manager.get_by_id(dataset.uuid, expected_type=Dataset)
    ds_wrapper = EditableDatasetTestWrapper(dataset=dataset, us_manager=us_manager)
    source_id = dataset.get_single_data_source_id()
    ds_wrapper.update_data_source(source_id=source_id, role=DataSourceRole.origin, table_name='qqqq')
    dsrc = ds_wrapper.get_data_source_strict(source_id=source_id, role=DataSourceRole.origin)
    conn_executor = sr.get_conn_executor_factory().get_sync_conn_executor(conn=dsrc.connection)
    with pytest.raises(exc.DLBaseException):
        dsrc.get_schema_info(conn_executor_factory=lambda: conn_executor)


def _check_source_exists(dataset, sync_usm, service_registry):
    dataset = sync_usm.get_by_id(dataset.uuid, expected_type=Dataset)
    ds_wrapper = DatasetTestWrapper(dataset=dataset, us_manager=sync_usm)
    dsrc = ds_wrapper.get_data_source_strict(
        source_id=dataset.get_single_data_source_id(), role=DataSourceRole.origin)
    conn_executor = service_registry.get_conn_executor_factory().get_sync_conn_executor(conn=dsrc.connection)
    assert dsrc.source_exists(conn_executor_factory=lambda: conn_executor)


def test_source_exists(default_sync_usm, saved_dataset, default_service_registry):
    _check_source_exists(
        saved_dataset, sync_usm=default_sync_usm,
        service_registry=default_service_registry,
    )


def test_source_exists_with_schema(default_sync_usm, saved_schematized_dataset, default_service_registry):
    _check_source_exists(
        saved_schematized_dataset, sync_usm=default_sync_usm,
        service_registry=default_service_registry,
    )


def test_source_exists_view(default_sync_usm, saved_dataset_for_view, default_service_registry):
    _check_source_exists(
        saved_dataset_for_view, sync_usm=default_sync_usm,
        service_registry=default_service_registry,
    )


def test_source_exists_view_with_schema(
        default_sync_usm, saved_schematized_dataset_for_view, default_service_registry,
):
    _check_source_exists(
        saved_schematized_dataset_for_view, sync_usm=default_sync_usm,
        service_registry=default_service_registry,
    )
