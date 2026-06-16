import pydantic

from dl_us_entries_client.models.entry import (
    Data,
    Entry,
    UnversionedData,
)
from dl_us_entries_client.models.extract import (
    ExtractProperties,
    UnversionedExtractProperties,
)


class DatasetData(Data):
    extract: ExtractProperties | None = None


class DatasetUnversionedData(UnversionedData):
    extract: UnversionedExtractProperties | None = None


class DatasetEntry(Entry):
    data: DatasetData | None = None
    unversioned_data: DatasetUnversionedData | None = pydantic.Field(
        default_factory=DatasetUnversionedData,
        alias="unversionedData",
    )
