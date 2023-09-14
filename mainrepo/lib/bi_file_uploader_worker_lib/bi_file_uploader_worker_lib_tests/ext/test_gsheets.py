import asyncio
import logging
import uuid

import pytest

from bi_constants.enums import (
    BIType,
    FileProcessingStatus,
)
from bi_core.us_manager.us_manager_async import AsyncUSManager
from bi_core_testing.connection import make_conn_key
from bi_file_uploader_lib import exc
from bi_file_uploader_lib.data_sink.json_each_row import S3JsonEachRowUntypedFileAsyncDataSink
from bi_file_uploader_lib.enums import FileType
from bi_file_uploader_lib.redis_model.base import RedisModelManager
from bi_file_uploader_lib.redis_model.models import (
    DataFile,
    DataSource,
    DataSourcePreview,
    GSheetsFileSourceSettings,
    GSheetsUserSourceDataSourceProperties,
    GSheetsUserSourceProperties,
)
from bi_file_uploader_task_interface.tasks import (
    DownloadGSheetTask,
    ParseFileTask,
    SaveSourceTask,
)
from bi_file_uploader_worker_lib.utils import parsing_utils
from bi_file_uploader_worker_lib.utils.converter_parsing_utils import idx_to_alphabet_notation
from bi_task_processor.state import wait_task

from bi_connector_bundle_chs3.chs3_gsheets.core.constants import CONNECTION_TYPE_GSHEETS_V2
from bi_connector_bundle_chs3.chs3_gsheets.core.us_connection import GSheetsFileS3Connection

LOGGER = logging.getLogger(__name__)


# BIType aliases
STR = BIType.string
INT = BIType.integer
F = BIType.float
D = BIType.date
DT = BIType.genericdatetime
B = BIType.boolean

SPREADHEET_ID = "1rnUFa7AiSKD5O80IKCvMy2cSZvLU1kRw9dxbtZbDMWc"

TEST_SHEET_TITLES_INDICES = {
    title: idx
    for idx, title in enumerate(
        (
            "image",
            "experiment",
            "corners",
            "elaborate",
            "empty titles",
            "elaborate (no number format)",
        )
    )
}

EXPECTED_BODY_TYPES = {  # { <sheet_title>: [ <user_type1>, <user_type2>, ... ] }
    "image": [],
    "experiment": [STR] * 10,
    "corners": [STR] * 5,
    "elaborate": [INT, D, F, STR, B, DT, STR, STR],
    "empty titles": [INT, STR, STR, STR, STR],
    "elaborate (no number format)": [STR] * 10,
}

EXPECTED_HEADERS = {  # { <sheet_title>: { <has_header>: [ <title1>, <title2>, ... ] } }
    "elaborate": {
        True: [
            "int field",
            "date field",
            "float field",
            "string field",
            "bool field",
            "datetime field",
            "time field",
            "time period field",
        ],
        False: ["A", "B", "C", "D", "E", "F", "G", "H"],
    },
    "empty titles": {
        True: ["int field", "B", "another field to test empty titles", "D", "E"],
        False: ["A", "B", "C", "D", "E"],
    },
}

_MOSCOWSHOPS_V8_TEST_PARAMS = [
    [True, True, False, True, False],
    [
        [
            "OrderID",
            "ClientID",
            "ProductID",
            "ShopID",
            "ClientAdress",
            "ShopAdress",
            "ShopName",
            "ProductBrend",
            "ProductCategory",
            "DayOfBirth",
            "OrderDatetime",
            "DeliveryDatetime",
            "Discount",
            "ProductCount",
            "DeliveryAdressCoord",
            "ShopAdressCoord",
            "ProductName",
            "ClientName",
            "DeliveryType",
            "PaymentType",
            "ProductPrice",
            "FinelAmount",
            "Gender",
            "ProductSubcategory",
            "Amount",
            "ClientStatus",
        ],
        [
            "OrderID",
            "ClientID",
            "ProductID",
            "ShopID",
            "OrderDatetime",
            "DeliveryDatetime",
            "Discount",
            "ProductCount",
            "DeliveryAdressCoord",
            "DeliveryType",
            "PaymentType",
            "Price",
            "FinelAmount",
            "Amount",
        ],
        [idx_to_alphabet_notation(i) for i in range(4)],  # ['ShopID', 'ShopName', 'ShopAdress', 'DeliveryAdressCoord'],
        ["ClientID", "ClientName", "Gender", "ClientStatus", "DayOfBirth", "ClientAdress", "BornBefore1980"],
        [
            idx_to_alphabet_notation(i) for i in range(5)
        ],  # ['ProductID', 'ProductCategory', 'ProductSubcategory', 'ProductBrend', 'ProductName'],
    ],
    [
        [STR] * 9 + [D, DT, STR, INT, INT] + [STR] * 6 + [F, INT, STR, STR, INT, STR],
        [STR, STR, STR, STR, DT, STR, INT, INT, STR, STR, STR, F, INT, INT],
        [STR, STR, STR, STR],
        [STR, STR, STR, STR, D, STR, B],
        [STR, STR, STR, STR, STR],
    ],
]

_big_gsheets_test_params = dict(
    argvalues=[
        (
            "1SEwxMwOqieNAgybrR_tGbt_N60mPp00T3LawutJ83RA",
            *_MOSCOWSHOPS_V8_TEST_PARAMS,
        ),
        (
            "1sPIpWUZa7wgUnUDa-MFPidJTSjQ40jS2OmD0g6V_wDw",
            *_MOSCOWSHOPS_V8_TEST_PARAMS,
        ),
        (
            "1OK6beKLQx8JqioiNTRN90OK1A9wN3bL6Xn0cwaWp0rw",
            *_MOSCOWSHOPS_V8_TEST_PARAMS,
        ),
        (
            "1G3BFi2Ai5jsNfazpwYCzZ5OUUiCMNpG_5BBMlFnpYy4",
            [False],
            [[idx_to_alphabet_notation(i) for i in range(17)]],
            [[D] + [STR] * 6 + [INT] * 8 + [F, B]],
        ),
    ],
    ids=[
        "MoscowShopsV8_tiny",
        "MoscowShopsV8_truncated",
        "MoscowShopsV8_full",
        "no_header",
    ],
)


@pytest.fixture(scope="function")
async def downloaded_gsheet_file_id(
    task_processor_client,
    task_state,
    s3_client,
    redis_model_manager,
    s3_tmp_bucket,
):
    df = DataFile(
        manager=redis_model_manager,
        filename="fu",
        status=FileProcessingStatus.in_progress,
        file_type=FileType.gsheets,
        user_source_properties=GSheetsUserSourceProperties(spreadsheet_id=SPREADHEET_ID),
    )
    await df.save()

    task = await task_processor_client.schedule(
        DownloadGSheetTask(file_id=df.id, authorized=False, schedule_parsing=False)
    )
    result = await wait_task(task, task_state)
    assert result[-1] == "success"

    yield df.id

    df = await DataFile.get(manager=redis_model_manager, obj_id=df.id)
    for source in df.sources:
        await s3_client.delete_object(
            Bucket=s3_tmp_bucket,
            Key=source.s3_key,
        )


@pytest.mark.asyncio
async def test_download_gsheet_task(
    task_processor_client,
    task_state,
    s3_client,
    redis_model_manager,
    s3_tmp_bucket,
):
    df = DataFile(
        manager=redis_model_manager,
        filename="fu",
        status=FileProcessingStatus.in_progress,
        file_type=FileType.gsheets,
        user_source_properties=GSheetsUserSourceProperties(spreadsheet_id=SPREADHEET_ID),
    )
    await df.save()

    task = await task_processor_client.schedule(
        DownloadGSheetTask(file_id=df.id, authorized=False, schedule_parsing=False)
    )
    result = await wait_task(task, task_state)
    assert result[-1] == "success"

    df = await DataFile.get(manager=redis_model_manager, obj_id=df.id)
    assert df.status == FileProcessingStatus.in_progress

    assert not df.sources[TEST_SHEET_TITLES_INDICES["image"]].is_applicable
    assert df.sources[TEST_SHEET_TITLES_INDICES["experiment"]].is_applicable
    assert df.sources[TEST_SHEET_TITLES_INDICES["corners"]].is_applicable
    assert df.sources[TEST_SHEET_TITLES_INDICES["elaborate"]].is_applicable
    assert df.sources[TEST_SHEET_TITLES_INDICES["empty titles"]].is_applicable
    assert df.sources[TEST_SHEET_TITLES_INDICES["elaborate (no number format)"]].is_applicable

    # test elaborate sheet types
    elaborate_source = df.sources[TEST_SHEET_TITLES_INDICES["elaborate"]]
    actual_user_types = [col.user_type for col in elaborate_source.raw_schema]
    expected_user_types = [
        BIType.integer,
        BIType.date,
        BIType.float,
        BIType.string,
        BIType.boolean,
        BIType.genericdatetime,
        BIType.string,
        BIType.string,
    ]
    assert actual_user_types == expected_user_types

    for source in df.sources:
        await s3_client.delete_object(
            Bucket=s3_tmp_bucket,
            Key=source.s3_key,
        )


@pytest.mark.asyncio
async def test_parse_gsheet(
    task_processor_client,
    task_state,
    s3_client,
    redis_model_manager,
    s3_tmp_bucket,
    downloaded_gsheet_file_id,
):
    file_id = downloaded_gsheet_file_id
    task = await task_processor_client.schedule(ParseFileTask(file_id=file_id))
    result = await wait_task(task, task_state)
    assert result[-1] == "success"

    df = await DataFile.get(manager=redis_model_manager, obj_id=file_id)
    assert df.status == FileProcessingStatus.ready

    elaborate_source_no_types = df.sources[TEST_SHEET_TITLES_INDICES["elaborate (no number format)"]]
    assert elaborate_source_no_types.status == FileProcessingStatus.ready

    actual_user_types = [col.user_type for col in elaborate_source_no_types.raw_schema]
    expected_user_types = [BIType.string] * 10
    assert actual_user_types == expected_user_types

    preview = await DataSourcePreview.get(manager=redis_model_manager, obj_id=elaborate_source_no_types.preview_id)
    assert preview.id == elaborate_source_no_types.preview_id
    assert preview.preview_data == [  # No number formats specified => everything is string => no header
        [
            "int field",
            "date normal",
            "date gsheet",
            "float field",
            "string field",
            "bool field",
            "dt normal",
            "dt gsheet",
            "time field",
            "time period field",
        ],
        [
            "1",
            "2001-07-31",
            "31.07.2001",
            "12,50",
            "asdf",
            "True",
            "2022-08-12 12:30:32",
            "12.08.2022 12:30:32",
            "12:00:12",
            "13:00:12",
        ],
        [None, None, None, None, None, None, None, None, None, None],
        [
            "2",
            "1995-04-12",
            "12.04.1995",
            "12,40",
            "zxcv",
            "True",
            "2022-08-14 12:30:34",
            "14.08.2022 12:30:34",
            "14:00:12",
            "15:00:12",
        ],
        [
            "8",
            "2008-02-29",
            "29.02.2008",
            None,
            "12,00",
            "True",
            "2022-08-15 12:30:35",
            "15.08.2022 12:30:35",
            "15:00:12",
            "16:00:12",
        ],
        [
            "3",
            "2012-03-12",
            "12.03.2012",
            "12,30",
            "dfgh",
            "True",
            "2022-08-16 12:30:36",
            "16.08.2022 12:30:36",
            "16:00:12",
            "17:00:12",
        ],
        [None, None, None, None, None, "StRiNg", None, None, None, "18:00:12"],
    ]


async def assert_parsing_results(
    file_id: str,
    has_header_expected: bool,
    rmm: RedisModelManager,
    dsrc_title: str,
    sheet_len: int,
) -> DataSource:
    df = await DataFile.get(manager=rmm, obj_id=file_id)
    assert df.status == FileProcessingStatus.ready

    dsrc = df.sources[TEST_SHEET_TITLES_INDICES[dsrc_title]]
    assert dsrc.status == FileProcessingStatus.ready

    file_source_settings = dsrc.file_source_settings
    assert isinstance(file_source_settings, GSheetsFileSourceSettings)

    preview = await DataSourcePreview.get(manager=rmm, obj_id=dsrc.preview_id)
    assert preview.id == dsrc.preview_id

    assert file_source_settings.first_line_is_header == has_header_expected
    if not has_header_expected:
        # mixed header => everything is string
        assert all(col.user_type == BIType.string for col in dsrc.raw_schema)
        assert len(preview.preview_data) == sheet_len
    else:
        # set header => correct type
        assert [sch_col.user_type for sch_col in dsrc.raw_schema] == EXPECTED_BODY_TYPES[dsrc_title]
        assert len(preview.preview_data) == sheet_len - 1  # preview becomes one line shorter

    assert [sch_col.title for sch_col in dsrc.raw_schema] == EXPECTED_HEADERS[dsrc_title][has_header_expected]

    return dsrc


@pytest.mark.asyncio
async def test_parse_gsheet_with_file_settings(
    task_processor_client,
    task_state,
    s3_client,
    redis_model_manager,
    s3_tmp_bucket,
    downloaded_gsheet_file_id,
):
    """Testing first_line_is_header override: None (which is False) -> True -> False"""

    file_id = downloaded_gsheet_file_id
    rmm = redis_model_manager
    sheet_len = 7
    sheet_title = "empty titles"

    task = await task_processor_client.schedule(ParseFileTask(file_id=file_id))
    result = await wait_task(task, task_state)
    assert result[-1] == "success"
    empty_titles_dsrc = await assert_parsing_results(file_id, False, rmm, sheet_title, sheet_len)

    task = await task_processor_client.schedule(
        ParseFileTask(
            file_id=file_id,
            source_id=empty_titles_dsrc.id,
            file_settings=dict(first_line_is_header=True),
            source_settings={},
        )
    )
    result = await wait_task(task, task_state)
    assert result[-1] == "success"
    empty_titles_dsrc = await assert_parsing_results(file_id, True, rmm, sheet_title, sheet_len)

    task = await task_processor_client.schedule(
        ParseFileTask(
            file_id=file_id,
            source_id=empty_titles_dsrc.id,
            file_settings=dict(first_line_is_header=False),
            source_settings={},
        )
    )
    result = await wait_task(task, task_state)
    assert result[-1] == "success"
    await assert_parsing_results(file_id, False, rmm, sheet_title, sheet_len)


@pytest.mark.asyncio
async def test_parse_gsheet_with_file_settings_reverse(
    task_processor_client,
    task_state,
    s3_client,
    redis_model_manager,
    s3_tmp_bucket,
    downloaded_gsheet_file_id,
):
    """Testing first_line_is_header override: None (which is True) -> False -> True"""

    file_id = downloaded_gsheet_file_id
    rmm = redis_model_manager
    sheet_len = 7
    sheet_title = "elaborate"

    task = await task_processor_client.schedule(ParseFileTask(file_id=file_id))
    result = await wait_task(task, task_state)
    assert result[-1] == "success"
    elaborate_no_number_format_dsrc = await assert_parsing_results(file_id, True, rmm, sheet_title, sheet_len)

    task = await task_processor_client.schedule(
        ParseFileTask(
            file_id=file_id,
            source_id=elaborate_no_number_format_dsrc.id,
            file_settings=dict(first_line_is_header=False),
            source_settings={},
        )
    )
    result = await wait_task(task, task_state)
    assert result[-1] == "success"
    elaborate_no_number_format_dsrc = await assert_parsing_results(file_id, False, rmm, sheet_title, sheet_len)

    task = await task_processor_client.schedule(
        ParseFileTask(
            file_id=file_id,
            source_id=elaborate_no_number_format_dsrc.id,
            file_settings=dict(first_line_is_header=True),
            source_settings={},
        )
    )
    result = await wait_task(task, task_state)
    assert result[-1] == "success"
    await assert_parsing_results(file_id, True, rmm, sheet_title, sheet_len)


@pytest.mark.asyncio
async def test_download_and_parse_gsheet(
    task_processor_client,
    task_state,
    s3_client,
    redis_model_manager,
    s3_tmp_bucket,
):
    df = DataFile(
        manager=redis_model_manager,
        filename="fu",
        status=FileProcessingStatus.in_progress,
        file_type=FileType.gsheets,
        user_source_properties=GSheetsUserSourceProperties(spreadsheet_id=SPREADHEET_ID),
    )
    await df.save()

    task = await task_processor_client.schedule(DownloadGSheetTask(file_id=df.id, authorized=False))
    result = await wait_task(task, task_state)
    assert result[-1] == "success"

    await asyncio.sleep(15)

    df = await DataFile.get(manager=redis_model_manager, obj_id=df.id)
    assert df.status == FileProcessingStatus.ready

    assert not df.sources[TEST_SHEET_TITLES_INDICES["image"]].is_applicable
    assert df.sources[TEST_SHEET_TITLES_INDICES["experiment"]].is_applicable
    assert df.sources[TEST_SHEET_TITLES_INDICES["corners"]].is_applicable
    assert df.sources[TEST_SHEET_TITLES_INDICES["elaborate"]].is_applicable
    assert df.sources[TEST_SHEET_TITLES_INDICES["empty titles"]].is_applicable
    assert df.sources[TEST_SHEET_TITLES_INDICES["elaborate (no number format)"]].is_applicable

    for source in df.sources:
        await s3_client.delete_object(
            Bucket=s3_tmp_bucket,
            Key=source.s3_key,
        )


async def create_gsheets_v2_connection(us_manager: AsyncUSManager, dfile: DataFile):
    conn_name = "gsheets_v2 test conn {}".format(uuid.uuid4())
    conn_sources = []
    for src in dfile.sources:
        if not src.is_applicable:
            continue
        user_source_properties = dfile.user_source_properties
        assert isinstance(user_source_properties, GSheetsUserSourceProperties)
        file_source_settings = src.file_source_settings
        assert isinstance(file_source_settings, GSheetsFileSourceSettings)
        user_source_dsrc_properties = src.user_source_dsrc_properties
        assert isinstance(user_source_dsrc_properties, GSheetsUserSourceDataSourceProperties)

        conn_sources.append(
            GSheetsFileS3Connection.FileDataSource(
                id=src.id,
                file_id=dfile.id,
                title=src.title,
                status=FileProcessingStatus.in_progress,
                spreadsheet_id=user_source_properties.spreadsheet_id,
                sheet_id=user_source_dsrc_properties.sheet_id,
                first_line_is_header=file_source_settings.first_line_is_header,
                raw_schema=src.raw_schema,
            )
        )
    data = GSheetsFileS3Connection.DataModel(sources=conn_sources, refresh_enabled=False)
    conn = GSheetsFileS3Connection.create_from_dict(
        data,
        ds_key=make_conn_key("connections", conn_name),
        type_=CONNECTION_TYPE_GSHEETS_V2.name,
        meta={"title": conn_name, "state": "saved"},
        us_manager=us_manager,
    )
    await us_manager.save(conn)
    return conn


@pytest.mark.skip(reason="Some US problem in CI.")  # TODO
@pytest.mark.asyncio
async def test_save_source_task(
    task_processor_client,
    task_state,
    s3_client,
    redis_model_manager,
    downloaded_gsheet_file_id,
    default_async_usm_per_test,
    s3_tmp_bucket,
):
    file_id = downloaded_gsheet_file_id
    usm = default_async_usm_per_test
    task = await task_processor_client.schedule(ParseFileTask(file_id=downloaded_gsheet_file_id))
    result = await wait_task(task, task_state)
    assert result[-1] == "success"

    df = await DataFile.get(manager=redis_model_manager, obj_id=file_id)
    assert df.sources[0].status == FileProcessingStatus.failed
    assert all(source.status == FileProcessingStatus.ready for source in df.sources[1:])

    conn = await create_gsheets_v2_connection(usm, df)
    assert all(
        conn.get_file_source_by_id(source.id).status == FileProcessingStatus.in_progress for source in df.sources[1:]
    )

    for src in df.sources:
        if not src.is_applicable:
            continue

        task_save = await task_processor_client.schedule(
            SaveSourceTask(
                tenant_id="common",
                file_id=file_id,
                src_source_id=src.id,
                dst_source_id=src.id,
                connection_id=conn.uuid,
            )
        )
        await wait_task(task_save, task_state)

        conn1 = await usm.get_by_id(conn.uuid)
        assert conn1.get_file_source_by_id(src.id).status == FileProcessingStatus.ready


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "spreadsheet_id, expected_has_header, expected_headers, expected_user_types",
    **_big_gsheets_test_params,
)
async def test_download_and_parse_big_gsheets(
    task_processor_client,
    task_state,
    s3_client,
    redis_model_manager,
    s3_tmp_bucket,
    spreadsheet_id,
    expected_has_header,
    expected_headers,
    expected_user_types,
):
    assert len(expected_has_header) == len(expected_headers) == len(expected_user_types), "malformed test data"

    df = DataFile(
        manager=redis_model_manager,
        filename="fu",
        status=FileProcessingStatus.in_progress,
        file_type=FileType.gsheets,
        user_source_properties=GSheetsUserSourceProperties(spreadsheet_id=spreadsheet_id),
    )
    await df.save()

    # Download and Parse
    task = await task_processor_client.schedule(DownloadGSheetTask(file_id=df.id, authorized=False))
    result = await wait_task(task, task_state)
    assert result[-1] == "success"

    await asyncio.sleep(15)  # let parse finish

    df = await DataFile.get(manager=redis_model_manager, obj_id=df.id)
    assert df.status == FileProcessingStatus.ready

    assert len(df.sources) == len(expected_headers)

    for src in df.sources:
        assert src.is_applicable

    for src, has_header, header_values, user_types in zip(
        df.sources, expected_has_header, expected_headers, expected_user_types
    ):
        assert src.file_source_settings.first_line_is_header == has_header, src.title

        assert len(src.raw_schema) == len(header_values), src.title

        actual_source_columns = [col.name for col in src.raw_schema]
        assert actual_source_columns == [idx_to_alphabet_notation(i) for i in range(len(actual_source_columns))]

        actual_header_values = [col.title for col in src.raw_schema]
        assert actual_header_values == header_values, src.title

        actual_user_types = [col.user_type for col in src.raw_schema]
        assert actual_user_types == user_types, src.title

    for source in df.sources:
        await s3_client.delete_object(
            Bucket=s3_tmp_bucket,
            Key=source.s3_key,
        )


@pytest.mark.skip(reason="Some US problem in CI.")  # TODO
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "spreadsheet_id, expected_has_header, expected_headers, expected_user_types",
    **_big_gsheets_test_params,
)
async def test_gsheets_full_pipeline(
    task_processor_client,
    task_state,
    s3_client,
    redis_model_manager,
    s3_tmp_bucket,
    s3_persistent_bucket,
    spreadsheet_id,
    expected_has_header,
    expected_headers,
    expected_user_types,
    default_async_usm_per_test,
):
    assert len(expected_has_header) == len(expected_headers) == len(expected_user_types), "malformed test data"

    df = DataFile(
        manager=redis_model_manager,
        filename="fu",
        status=FileProcessingStatus.in_progress,
        file_type=FileType.gsheets,
        user_source_properties=GSheetsUserSourceProperties(spreadsheet_id=spreadsheet_id),
    )
    await df.save()

    # Download and Parse
    task = await task_processor_client.schedule(DownloadGSheetTask(file_id=df.id, authorized=False))
    result = await wait_task(task, task_state)
    assert result[-1] == "success"

    await asyncio.sleep(5)  # let parse finish

    df = await DataFile.get(manager=redis_model_manager, obj_id=df.id)
    assert df.status == FileProcessingStatus.ready

    assert len(df.sources) == len(expected_headers)

    for src in df.sources:
        assert src.is_applicable

    for src, has_header, header_values, user_types in zip(
        df.sources, expected_has_header, expected_headers, expected_user_types
    ):
        assert src.file_source_settings.first_line_is_header == has_header, src.title

        assert len(src.raw_schema) == len(header_values), src.title

        actual_source_columns = [col.name for col in src.raw_schema]
        assert actual_source_columns == [idx_to_alphabet_notation(i) for i in range(len(actual_source_columns))]

        actual_header_values = [col.title for col in src.raw_schema]
        assert actual_header_values == header_values, src.title

        actual_user_types = [col.user_type for col in src.raw_schema]
        assert actual_user_types == user_types, src.title

    # Save
    file_id = df.id
    usm = default_async_usm_per_test
    task = await task_processor_client.schedule(ParseFileTask(file_id=file_id))
    result = await wait_task(task, task_state)
    assert result[-1] == "success"

    df = await DataFile.get(manager=redis_model_manager, obj_id=file_id)
    assert all(source.status == FileProcessingStatus.ready for source in df.sources)

    conn = await create_gsheets_v2_connection(usm, df)
    assert all(
        conn.get_file_source_by_id(source.id).status == FileProcessingStatus.in_progress for source in df.sources
    )

    for src in df.sources:
        task_save = await task_processor_client.schedule(
            SaveSourceTask(
                tenant_id="common",
                file_id=file_id,
                src_source_id=src.id,
                dst_source_id=src.id,
                connection_id=conn.uuid,
            )
        )
        result = await wait_task(task_save, task_state)
        assert result[-1] == "success"

        updated_conn: GSheetsFileS3Connection = await usm.get_by_id(conn.uuid)
        assert updated_conn.get_file_source_by_id(src.id).status == FileProcessingStatus.ready

        for conn_src, has_header, header_values, user_types in zip(
            updated_conn.data.sources, expected_has_header, expected_headers, expected_user_types
        ):
            assert conn_src.first_line_is_header == has_header, src.title

            assert len(conn_src.raw_schema) == len(header_values), src.title

            actual_source_columns = [col.name for col in conn_src.raw_schema]
            assert actual_source_columns == [idx_to_alphabet_notation(i) for i in range(len(actual_source_columns))]

            actual_header_values = [col.title for col in conn_src.raw_schema]
            assert actual_header_values == header_values, src.title

            actual_user_types = [col.user_type for col in conn_src.raw_schema]
            assert actual_user_types == user_types, src.title

    for source in df.sources:
        await s3_client.delete_object(
            Bucket=s3_tmp_bucket,
            Key=source.s3_key,
        )


@pytest.mark.asyncio
async def test_too_many_columns_gsheets(
    task_processor_client,
    task_state,
    s3_client,
    redis_model_manager,
    s3_tmp_bucket,
    monkeypatch,
):
    monkeypatch.setattr(parsing_utils, "MAX_COLUMNS_COUNT", 5)

    df = DataFile(
        manager=redis_model_manager,
        filename="too_many_columns",
        status=FileProcessingStatus.in_progress,
        file_type=FileType.gsheets,
        user_source_properties=GSheetsUserSourceProperties(spreadsheet_id=SPREADHEET_ID),
    )
    await df.save()

    task = await task_processor_client.schedule(DownloadGSheetTask(file_id=df.id, authorized=False))
    result = await wait_task(task, task_state)
    assert result[-1] == "success"

    await asyncio.sleep(5)

    df = await DataFile.get(manager=redis_model_manager, obj_id=df.id)
    assert df.status == FileProcessingStatus.ready

    assert not df.sources[TEST_SHEET_TITLES_INDICES["image"]].is_applicable
    assert not df.sources[TEST_SHEET_TITLES_INDICES["experiment"]].is_applicable
    assert df.sources[TEST_SHEET_TITLES_INDICES["corners"]].is_applicable
    assert not df.sources[TEST_SHEET_TITLES_INDICES["elaborate"]].is_applicable
    assert df.sources[TEST_SHEET_TITLES_INDICES["empty titles"]].is_applicable
    assert not df.sources[TEST_SHEET_TITLES_INDICES["elaborate (no number format)"]].is_applicable

    too_many_cols_sources_indices = (
        TEST_SHEET_TITLES_INDICES["experiment"],
        TEST_SHEET_TITLES_INDICES["elaborate"],
        TEST_SHEET_TITLES_INDICES["elaborate (no number format)"],
    )

    assert df.sources[TEST_SHEET_TITLES_INDICES["image"]].error.code == exc.EmptyDocument.err_code
    for idx in too_many_cols_sources_indices:
        assert df.sources[idx].error.code == exc.TooManyColumnsError.err_code, df.sources[idx].title

    for source in df.sources:
        await s3_client.delete_object(
            Bucket=s3_tmp_bucket,
            Key=source.s3_key,
        )


@pytest.mark.asyncio
async def test_too_large_sheet(
    task_processor_client,
    task_state,
    s3_client,
    redis_model_manager,
    s3_tmp_bucket,
    monkeypatch,
):
    monkeypatch.setattr(S3JsonEachRowUntypedFileAsyncDataSink, "max_file_size_bytes", 8 * 1024**2)

    df = DataFile(
        manager=redis_model_manager,
        filename="too_large_sheet",
        status=FileProcessingStatus.in_progress,
        file_type=FileType.gsheets,
        user_source_properties=GSheetsUserSourceProperties(
            spreadsheet_id="1dfUajkRxGTxGXrwjXtCRWhEjHlZX9tMVEvla5fio93w"
        ),
    )
    await df.save()

    task = await task_processor_client.schedule(DownloadGSheetTask(file_id=df.id, authorized=False))
    result = await wait_task(task, task_state)
    assert result[-1] == "success"

    await asyncio.sleep(5)

    df = await DataFile.get(manager=redis_model_manager, obj_id=df.id)
    assert df.status == FileProcessingStatus.ready

    assert not df.sources[0].is_applicable
    assert df.sources[1].is_applicable

    assert df.sources[0].error.code == exc.FileLimitError.err_code
    assert df.sources[1].error is None

    for source in df.sources:
        await s3_client.delete_object(
            Bucket=s3_tmp_bucket,
            Key=source.s3_key,
        )


@pytest.mark.asyncio
async def test_url_params_encoding(
    task_processor_client,
    task_state,
    s3_client,
    redis_model_manager,
    s3_tmp_bucket,
    monkeypatch,
):
    df = DataFile(
        manager=redis_model_manager,
        filename="slashes and stuff",
        status=FileProcessingStatus.in_progress,
        file_type=FileType.gsheets,
        user_source_properties=GSheetsUserSourceProperties(
            spreadsheet_id="1dE5-owygpjK-F65qlCwUa1YHGzh4dzBhyajURSLMvYU"
        ),
    )
    await df.save()

    task = await task_processor_client.schedule(
        DownloadGSheetTask(file_id=df.id, authorized=False, schedule_parsing=False)
    )
    result = await wait_task(task, task_state)
    assert result[-1] == "success"
