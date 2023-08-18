from __future__ import annotations

import uuid

import pytest
import sqlalchemy as sa

from bi_constants.enums import ConnectionType, DataSourceRole

from bi_core.exc import CHYDBQueryError
from bi_core.query.expression import ExpressionCtx
from bi_core.query.bi_query import BIQuery
from bi_core_testing.dataset import make_dataset
from bi_core_testing.data import DataFetcher
from bi_core.connectors.chydb.us_connection import ConnectionCHYDB
from bi_core.us_dataset import Dataset
from bi_core.us_manager.us_manager_sync import SyncUSManager
from bi_core_testing.dataset_wrappers import DatasetTestWrapper

from bi_legacy_test_bundle_tests.core.ext.chydb.config import CHYDB_TEST_DATASET_PARAMS_BASE


class CHYDBTestBase:

    @pytest.fixture
    def chydb_test_connection_params(self, chydb_test_connection_params_base):
        return dict(
            chydb_test_connection_params_base,
            secure=False,
            default_ydb_cluster='ru',
            name='chydb conn {}'.format(uuid.uuid4()),
        )

    def make_saved_chydb_connection(self, sync_usm: SyncUSManager, params, **kwargs):
        conn = ConnectionCHYDB.create_from_dict(
            ConnectionCHYDB.DataModel(**params),
            ds_key=params['name'],
            type_=ConnectionType.chydb.name,
            us_manager=sync_usm,
            **kwargs)
        sync_usm.save(conn)
        return conn

    @pytest.yield_fixture(scope='function')
    def saved_chydb_connection(self, ext_sync_usm, app_context, chydb_test_connection_params):
        sync_usm = ext_sync_usm
        conn = self.make_saved_chydb_connection(
            sync_usm=sync_usm,
            params=chydb_test_connection_params)
        try:
            yield conn
        finally:
            sync_usm.delete(conn)

    @pytest.fixture
    def chydb_test_dataset_params(self, saved_chydb_connection, default_sync_usm):
        return dict(
            CHYDB_TEST_DATASET_PARAMS_BASE,
            sync_usm=default_sync_usm,
            connection=saved_chydb_connection,
        )

    @pytest.yield_fixture(scope='function')
    def saved_chydb_dataset(self, default_sync_usm, chydb_test_dataset_params, app_context):
        dataset = make_dataset(**chydb_test_dataset_params)
        default_sync_usm.save(dataset)
        try:
            yield dataset
        finally:
            default_sync_usm.delete(dataset)

    def _test_connection_params(self, saved_chydb_connection):
        return saved_chydb_connection.test()


class TestCHYDB(CHYDBTestBase):

    # @pytest.mark.todo  # WIP connection, currently.
    def test_select_data_chydb(self, default_sync_usm, saved_chydb_dataset, default_async_service_registry):
        us_manager = default_sync_usm
        dataset = us_manager.get_by_id(saved_chydb_dataset.uuid, expected_type=Dataset)
        us_manager.load_dependencies(dataset)
        ds_wrapper = DatasetTestWrapper(dataset=dataset, us_manager=us_manager)

        avatar_id = ds_wrapper.get_root_avatar_strict().id
        role = DataSourceRole.origin
        columns = ds_wrapper.get_cached_raw_schema(role=DataSourceRole.origin)
        bi_query = BIQuery(
            select_expressions=[
                ExpressionCtx(
                    expression=sa.literal_column(col.name),
                    avatar_ids=[avatar_id],
                    user_type=col.user_type,
                    alias=col.name,
                )
                for col in columns],
            limit=10001,
        )
        data_fetcher = DataFetcher(
            service_registry=default_async_service_registry,
            dataset=dataset, us_manager=us_manager,
        )
        data = list(data_fetcher.get_data_stream(role=role, bi_query=bi_query).data)
        assert data


class TestCHYDBTokenErrorHandling(CHYDBTestBase):

    def make_saved_chydb_connection(self, sync_usm: SyncUSManager, params, **kwargs):
        params = dict(
            params,
            token='AQAD-qwe',
        )
        return super().make_saved_chydb_connection(sync_usm=sync_usm, params=params, **kwargs)

    def test_chydb_error(self, chydb_test_dataset_params):
        with pytest.raises(CHYDBQueryError):
            make_dataset(**chydb_test_dataset_params)
