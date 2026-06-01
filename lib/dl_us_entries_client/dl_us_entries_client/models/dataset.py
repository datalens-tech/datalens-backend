from dl_us_entries_client.models.entry import (
    Data,
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
