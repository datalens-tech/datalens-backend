import abc
from itertools import chain

from dl_api_client.dsmaker.data_abstraction.mapping_base import DataCellMapper1D


class Mapper1DComparator(abc.ABC):
    @abc.abstractmethod
    def assert_equal(self, left: DataCellMapper1D, right: DataCellMapper1D) -> None:
        raise NotImplementedError


class SimpleMapper1DComparator(Mapper1DComparator):
    def assert_equal(self, left: DataCellMapper1D, right: DataCellMapper1D) -> None:
        left_dict = left.as_dict()
        right_dict = right.as_dict()

        for key in set(chain(left_dict.keys(), right_dict.keys())):
            left_item = left_dict.get(key)
            right_item = right_dict.get(key)
            left_cell = left_item.cell if left_item is not None else None
            right_cell = right_item.cell if right_item is not None else None
            assert left_cell == right_cell, f"Value mismatch for key {key}: {left_cell} vs. {right_cell}"
