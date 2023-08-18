import csv

from bi_file_uploader_lib.enums import CSVEncoding

from bi_file_uploader_worker_lib.utils.parsing_utils import prepare_preview, make_upcropped_text_sample


def test_prepare_preview(sample_data_bytes_utf8: bytes):
    orig_rows = len(sample_data_bytes_utf8.decode('utf-8').splitlines())
    sample_str = make_upcropped_text_sample(sample_data_bytes_utf8, len(sample_data_bytes_utf8), CSVEncoding.utf8)
    assert len(sample_str.splitlines()) == orig_rows - 1    # without last row
    preview_data = prepare_preview(sample_str, csv.excel, True)
    assert len(preview_data) == 30
