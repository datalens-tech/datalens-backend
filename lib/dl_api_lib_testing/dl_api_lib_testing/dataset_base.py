import abc
from typing import Generator

import pytest

from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib_testing.connection_base import ConnectionTestBase


class DatasetTestBase(ConnectionTestBase, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    @pytest.fixture(scope="session")
    def dataset_params(self) -> dict:
        raise NotImplementedError

    def make_basic_dataset(
        self,
        control_api: SyncHttpDatasetApiV1,
        connection_id: str,
        dataset_params: dict,
    ) -> Dataset:
        ds = Dataset()
        ds.sources["source_1"] = ds.source(
            connection_id=connection_id,
            **dataset_params,
        )

        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
        ds = control_api.apply_updates(dataset=ds, fail_ok=False).dataset
        ds = control_api.save_dataset(dataset=ds).dataset
        return ds

    @pytest.fixture(scope="function")
    def saved_dataset(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_connection_id: str,
        dataset_params: dict,
    ) -> Generator[Dataset, None, None]:
        ds = self.make_basic_dataset(
            control_api=control_api,
            connection_id=saved_connection_id,
            dataset_params=dataset_params,
        )
        yield ds
        control_api.delete_dataset(dataset_id=ds.id, fail_ok=False)
