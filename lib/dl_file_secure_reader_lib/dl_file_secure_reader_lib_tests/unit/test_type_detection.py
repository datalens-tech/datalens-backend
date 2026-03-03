import io
from pathlib import Path

from dl_constants.enums import UserDataType
from dl_file_secure_reader_lib.resources.reader import parse_excel_data
from dl_file_uploader_worker_lib.utils.converter_parsing_utils import guess_types_and_header_excel


def test_excel_type_detection_cast(test_types_excel_file: Path) -> None:
    with open(test_types_excel_file, "rb") as f:
        excel_data = parse_excel_data(io.BytesIO(f.read()), feature_excel_read_only=True)

    sheet_data = excel_data[0]["data"]

    has_header, merged_types, header_types, column_types = guess_types_and_header_excel(iter(sheet_data))

    assert has_header is True
    assert len(merged_types) == 6

    expected_types = [
        UserDataType.integer,
        UserDataType.float,
        UserDataType.string,
        UserDataType.genericdatetime,  # This is date column, but openpyxl detects it as datetime
        UserDataType.genericdatetime,
        UserDataType.boolean,
    ]

    for i, expected_type in enumerate(expected_types):
        column_info = merged_types[i]
        assert column_info["cast"] == expected_type.name
