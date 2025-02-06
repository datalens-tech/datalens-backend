from __future__ import annotations

import abc

import attr

from dl_api_lib.dataset.validator import DatasetValidator
from dl_core.us_dataset import Dataset
from dl_core.us_manager.us_manager import USManagerBase


@attr.s(frozen=True)
class DatasetValidatorFactory(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_dataset_validator(
        self, ds: Dataset, us_manager: USManagerBase, is_data_api: bool = False
    ) -> DatasetValidator:
        pass


@attr.s(frozen=True)
class DefaultDatasetValidatorFactory(DatasetValidatorFactory):
    def get_dataset_validator(
        self, ds: Dataset, us_manager: USManagerBase, is_data_api: bool = False
    ) -> DatasetValidator:
        return DatasetValidator(
            ds=ds,
            us_manager=us_manager,
            is_data_api=is_data_api,
        )
