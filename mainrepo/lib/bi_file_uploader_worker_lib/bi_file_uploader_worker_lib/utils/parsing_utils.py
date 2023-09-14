import abc
import csv
import io
import json
import logging
import sys
from typing import (
    BinaryIO,
    Iterator,
    Optional,
    Type,
    final,
)

import attr
import cchardet as chardet

from bi_app_tools.profiling_base import generic_profiler
from bi_constants.enums import (
    BIType,
    ConnectionType,
)
from bi_core.aio.web_app_services.gsheets import Sheet
from bi_core.components.ids import (
    ID_LENGTH,
    ID_VALID_SYMBOLS,
    make_readable_field_id,
)
from bi_core.db import get_type_transformer
from bi_core.db.elements import SchemaColumn
from bi_file_uploader_lib import exc
from bi_file_uploader_lib.enums import (
    CSVDelimiter,
    CSVEncoding,
    FileType,
)
from bi_file_uploader_worker_lib.utils import converter_parsing_utils
from bi_file_uploader_worker_lib.utils.converter_parsing_utils import (
    idx_to_alphabet_notation,
    make_result_types,
    merge_column_types,
    raw_schema_to_column_types,
)

from bi_connector_bundle_chs3.chs3_gsheets.core.constants import CONNECTION_TYPE_GSHEETS_V2
from bi_connector_bundle_chs3.file.core.constants import CONNECTION_TYPE_FILE

LOGGER = logging.getLogger(__name__)


MAX_PREVIEW_ROWS = 30
MAX_PREVIEW_SIZE = 100 * 1024  # 100 KByte
MAX_PREVIEW_CELL_LEN = 40
MAX_COLUMNS_COUNT = 300


@attr.s(auto_attribs=True, kw_only=True)
class CSVDialect:
    delimiter: str
    doublequote: bool
    skipinitialspace: bool
    escapechar: Optional[str] = None
    lineterminator: str = "\r\n"
    quotechar: Optional[str] = '"'
    quoting: int = csv.QUOTE_MINIMAL


@generic_profiler("detect-encoding")
def detect_encoding(sample: bytes) -> CSVEncoding:
    encoding_resp = chardet.detect(sample).get("encoding")

    LOGGER.info(f'Run chardet.detect. Detected encoding "{encoding_resp}".')

    if not encoding_resp:
        LOGGER.info('Can\'t detect file encoding. Falling back to "utf-8".')
        encoding = CSVEncoding.utf8
    else:
        encoding_resp = encoding_resp.lower()
        try:
            encoding = CSVEncoding(encoding_resp)
        except ValueError:
            if encoding_resp == "ascii":
                LOGGER.info('replaced encoding "ascii" to "utf-8"')
                encoding = CSVEncoding.utf8
            elif "windows" in encoding_resp:
                LOGGER.info(f'replaced encoding "{encoding_resp}" to "windows1251"')
                encoding = CSVEncoding.windows1251
            else:
                LOGGER.info(f'Unexpected encoding: {encoding_resp}. Falling back to "utf-8".')
                encoding = CSVEncoding.utf8

    return encoding


def make_upcropped_text_sample(sample: bytes, expected_sample_len: int, encoding: CSVEncoding) -> str:
    sample_text = sample.decode(encoding.value, errors="replace")
    if len(sample) == expected_sample_len:
        # That means, the last line may be cropped in the middle. It's better to drop it.
        sample_lines = sample_text.splitlines(keepends=True)[:-1]
        sample_text = "".join(sample_lines)
    return sample_text


@generic_profiler("detect-dialect")
def detect_dialect(sample: str) -> CSVDialect:
    allowed_delimiters = "".join((d.value for d in CSVDelimiter))

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=allowed_delimiters)
    except csv.Error:
        LOGGER.info("Could not determine csv delimiter. Falling back to excel csv dialect.", exc_info=True)
        dialect = csv.excel

    local_dialect = CSVDialect(
        doublequote=dialect.doublequote,
        delimiter=dialect.delimiter,
        quotechar=dialect.quotechar,
        skipinitialspace=dialect.skipinitialspace,
    )

    return local_dialect


@attr.s
class FileUploaderFieldIdGenerator(metaclass=abc.ABCMeta):
    _id_formatter: str = "{}_{}"

    _seen_ids: set[str] = attr.ib(factory=set)

    @abc.abstractmethod
    def _make_id(self, field: converter_parsing_utils.TResultColumn) -> str:
        raise NotImplementedError

    def _resolve_id_collisions(self, item: str) -> str:
        idx = 1
        orig_item = item
        while item in self._seen_ids:
            item = self._id_formatter.format(orig_item, idx)
            idx += 1
        return item

    @final
    def make_field_id(self, field: converter_parsing_utils.TResultColumn) -> str:
        field_id = self._make_id(field)
        field_id = self._resolve_id_collisions(field_id)
        self._seen_ids.add(field_id)
        return field_id


@attr.s
class FileFieldIdGenerator(FileUploaderFieldIdGenerator):
    def _make_id(self, field: converter_parsing_utils.TResultColumn) -> str:
        return make_readable_field_id(field["title"], ID_VALID_SYMBOLS, ID_LENGTH)  # type: ignore  # TODO: FIX


@attr.s
class GSheetsFieldIdGenerator(FileUploaderFieldIdGenerator):
    def _make_id(self, field: converter_parsing_utils.TResultColumn) -> str:
        return idx_to_alphabet_notation(field["index"])  # type: ignore  # TODO: FIX


def get_field_id_generator(conn_type: ConnectionType) -> FileUploaderFieldIdGenerator:
    field_id_gen_cls_map: dict[ConnectionType, Type[FileUploaderFieldIdGenerator]] = {
        CONNECTION_TYPE_FILE: FileFieldIdGenerator,
        CONNECTION_TYPE_GSHEETS_V2: GSheetsFieldIdGenerator,
    }

    if conn_type not in field_id_gen_cls_map:
        raise ValueError(f'No field id generator specified for connection type "{conn_type.name}"')

    return field_id_gen_cls_map[conn_type]()


def result_column_types_to_raw_schema(
    column_types: converter_parsing_utils.TResultTypes,
    conn_type: ConnectionType,
) -> list[SchemaColumn]:
    field_id_gen = get_field_id_generator(conn_type)
    type_transformer = get_type_transformer(conn_type)

    raw_schema: list[SchemaColumn] = []
    for col in column_types:
        user_type = getattr(BIType, col["cast"])  # type: ignore  # TODO: FIX
        title: str = col["title"]  # type: ignore  # TODO: FIX
        sch_col = SchemaColumn(
            name=field_id_gen.make_field_id(col),
            title=title,
            user_type=user_type,
            native_type=type_transformer.type_user_to_native(user_type),
        )
        raw_schema.append(sch_col)

    return raw_schema


@generic_profiler("guess-header-and-schema")
def guess_header_and_schema(
    binary_data_stream: BinaryIO,
    encoding: CSVEncoding,
    dialect: csv.Dialect,
    has_header: Optional[bool] = None,
) -> tuple[bool, list[SchemaColumn]]:
    text_io_wrapper = io.TextIOWrapper(binary_data_stream, encoding=encoding.value, newline="")

    csv.field_size_limit(sys.maxsize)  # TODO: wrap csv.Error exception if limit exceeded

    csv_reader = csv.reader(text_io_wrapper, dialect)
    new_has_header, legacy_column_types = converter_parsing_utils.guess_types_and_header(
        csv_reader, has_header=has_header
    )

    raw_schema = result_column_types_to_raw_schema(legacy_column_types, CONNECTION_TYPE_FILE)

    if len(raw_schema) > MAX_COLUMNS_COUNT:
        raise exc.TooManyColumnsError()

    return new_has_header, raw_schema


@generic_profiler("guess-header-and-schema-gsheet")
def guess_header_and_schema_gsheet(
    sheet: Sheet,
) -> tuple[bool, list[SchemaColumn], list[SchemaColumn], list[SchemaColumn]]:
    (
        has_header,
        legacy_column_types,
        col_types_header,
        col_types_body,
    ) = converter_parsing_utils.guess_types_and_header_gsheets(
        data_iter=iter(sheet.data or ()),
        sample_lines_count=None,
    )

    raw_schema = result_column_types_to_raw_schema(legacy_column_types, CONNECTION_TYPE_GSHEETS_V2)

    if len(raw_schema) > MAX_COLUMNS_COUNT:
        raise exc.TooManyColumnsError()

    raw_schema_header = result_column_types_to_raw_schema(col_types_header, CONNECTION_TYPE_GSHEETS_V2)
    raw_schema_body = result_column_types_to_raw_schema(col_types_body, CONNECTION_TYPE_GSHEETS_V2)

    return has_header, raw_schema, raw_schema_header, raw_schema_body


@generic_profiler("guess-schema-gsheet")
def guess_schema_gsheet(sheet: Sheet) -> list[SchemaColumn]:
    col_types = converter_parsing_utils.guess_types_gsheets(
        data_iter=iter(sheet.data or ()),
        sample_lines_count=None,
    )

    raw_schema = result_column_types_to_raw_schema(col_types, CONNECTION_TYPE_GSHEETS_V2)

    if len(raw_schema) > MAX_COLUMNS_COUNT:
        raise exc.TooManyColumnsError()

    return raw_schema


def merge_raw_schemas_spreadsheet(
    header_rs: list[SchemaColumn],
    body_rs: list[SchemaColumn],
    has_header: bool,
    file_type: FileType,
) -> list[SchemaColumn]:
    conn_type_map: dict[FileType, ConnectionType] = {
        FileType.xlsx: CONNECTION_TYPE_FILE,
        FileType.gsheets: CONNECTION_TYPE_GSHEETS_V2,
    }
    col_types_header = raw_schema_to_column_types(header_rs)
    col_types_body = raw_schema_to_column_types(body_rs)

    if has_header:
        header = [col.title for col in header_rs]
        result_types = make_result_types(header, col_types_body, has_header, idx_to_alphabet_notation)
    else:
        header = [col.title for col in body_rs]
        merged_col_types = merge_column_types(col_types_header, col_types_body, has_header)
        result_types = make_result_types(header, merged_col_types, has_header, idx_to_alphabet_notation)

    return result_column_types_to_raw_schema(result_types, conn_type_map[file_type])


@generic_profiler("reguess-header-and-schema-gsheet")
def reguess_header_and_schema_spreadsheet(
    raw_schema_header: list[SchemaColumn],
    raw_schema_body: list[SchemaColumn],
    has_header: bool,
    file_type: FileType,
) -> tuple[bool, list[SchemaColumn]]:
    raw_schema = merge_raw_schemas_spreadsheet(raw_schema_header, raw_schema_body, has_header, file_type)
    return has_header, raw_schema


def reduce_preview_size_inplace(preview: list[list[Optional[str]]]) -> None:
    LOGGER.info("Trying to shorten cells to decrease preview size.")
    new_size = 0
    for row in preview:
        for cell_index, cell in enumerate(row):
            if cell is None:
                continue
            if len(cell) > MAX_PREVIEW_CELL_LEN:
                cropped_cell = f"{cell[:10]}...{cell[-10:]}"
                row[cell_index] = cropped_cell
                new_size += len(cropped_cell.encode("utf-8"))
            else:
                new_size += len(cell.encode("utf-8"))
    LOGGER.info(f"Preview size after shorten attempt: {new_size} bytes.")


@generic_profiler("prepare-preview")
def prepare_preview(sample: str, dialect: csv.Dialect, has_header: bool) -> list[list[Optional[str]]]:
    csv.field_size_limit(sys.maxsize)  # TODO: wrap csv.Error exception if limit exceeded

    csv_reader = csv.reader(io.StringIO(sample, newline=""), dialect=dialect)

    if has_header:
        # Skip header from preview
        next(csv_reader)

    preview: list[list[Optional[str]]] = []
    preview_size = 0
    for row in csv_reader:
        preview.append(row)  # type: ignore
        if len(preview) >= MAX_PREVIEW_ROWS:
            break

    for preview_row in preview:
        for cell_index, cell in enumerate(preview_row):
            cell_str = cell if cell is not None else ""
            preview_size += len(cell_str.encode("utf-8"))

            if cell_str == "":
                preview_row[cell_index] = None

    LOGGER.info(f"Collected {len(preview)} preview rows. Preview size: {preview_size} bytes.")

    if preview_size > MAX_PREVIEW_SIZE:
        reduce_preview_size_inplace(preview)

    return preview


def prepare_preview_from_json_each_row(sample: bytes, has_header: bool) -> list[list[Optional[str]]]:
    sample_text = sample.decode("utf-8", errors="replace")
    sample_lines = iter(sample_text.splitlines())
    preview: list[list[Optional[str]]] = []
    preview_size = 0
    if has_header:
        next(sample_lines)
    for line in sample_lines:
        try:
            row_data = json.loads(line)
        except json.JSONDecodeError:
            continue
        raw_row_data = [str(value) if value is not None else "" for value in row_data]
        preview_size += sum(len(value.encode("utf-8")) for value in raw_row_data)
        preview.append([cell if cell != "" else None for cell in raw_row_data])
        if len(preview) >= MAX_PREVIEW_ROWS:
            break

    LOGGER.info(f"Collected {len(preview)} preview rows. Preview size: {preview_size} bytes.")

    if preview_size > MAX_PREVIEW_SIZE:
        reduce_preview_size_inplace(preview)

    return preview


def get_csv_raw_data_iterator(
    binary_data_stream: BinaryIO,
    encoding: CSVEncoding,
    dialect: csv.Dialect,
    first_line_is_header: bool,
    raw_schema: list[SchemaColumn],
) -> Iterator:
    text_io_wrapper = io.TextIOWrapper(binary_data_stream, encoding=encoding.value, newline="")

    csv.field_size_limit(sys.maxsize)  # TODO: wrap csv.Error exception if limit exceeded

    csv_reader = csv.DictReader(
        text_io_wrapper,
        fieldnames=tuple(sch.name for sch in raw_schema),
        dialect=dialect,
    )

    if first_line_is_header:
        next(csv_reader)

    return csv_reader


@generic_profiler("guess_header_and_schema_excel")
def guess_header_and_schema_excel(
    data: list,
) -> tuple[bool, list[SchemaColumn], list[SchemaColumn], list[SchemaColumn]]:
    (
        has_header,
        legacy_column_types,
        col_types_header,
        col_types_body,
    ) = converter_parsing_utils.guess_types_and_header_excel(
        data_iter=iter(data),
        sample_lines_count=None,
    )

    raw_schema = result_column_types_to_raw_schema(legacy_column_types, CONNECTION_TYPE_FILE)

    if len(raw_schema) > MAX_COLUMNS_COUNT:
        raise ValueError("Too many columns")  # TODO: proper exc type

    raw_schema_header = result_column_types_to_raw_schema(col_types_header, CONNECTION_TYPE_FILE)
    raw_schema_body = result_column_types_to_raw_schema(col_types_body, CONNECTION_TYPE_FILE)

    return has_header, raw_schema, raw_schema_header, raw_schema_body
