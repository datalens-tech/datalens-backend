import abc

import pytest

from bi_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase
from bi_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from bi_api_client.dsmaker.primitives import Dataset

from bi_api_lib_testing.connection_base import ConnectionTestBase


class DatasetTestBase(ConnectionTestBase, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    @pytest.fixture(scope='session')
    def dataset_params(self) -> dict:
        raise NotImplementedError

    @pytest.fixture(scope='function')
    def dataset_api(self, control_api_sync_client: SyncHttpClientBase) -> SyncHttpDatasetApiV1:
        return SyncHttpDatasetApiV1(client=control_api_sync_client)

    def make_basic_dataset(
            self,
            dataset_api: SyncHttpDatasetApiV1,
            connection_id: str,
            dataset_params: dict,
    ) -> Dataset:
        ds = Dataset()
        ds.sources['source_1'] = ds.source(
            connection_id=connection_id,
            **dataset_params,
        )

        ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
        ds = dataset_api.apply_updates(dataset=ds, fail_ok=False).dataset
        ds = dataset_api.save_dataset(dataset=ds).dataset
        return ds

    @pytest.fixture(scope='function')
    def saved_dataset(
            self, dataset_api: SyncHttpDatasetApiV1,
            saved_connection_id: str,
            dataset_params: dict,
    ) -> Dataset:
        ds = self.make_basic_dataset(
            dataset_api=dataset_api, connection_id=saved_connection_id,
            dataset_params=dataset_params,
        )
        return ds
