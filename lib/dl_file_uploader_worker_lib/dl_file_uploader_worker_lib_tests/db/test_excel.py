from asyncio import sleep
import logging

import pytest

from dl_constants.enums import (
    FileProcessingStatus,
    UserDataType,
)
from dl_file_uploader_lib.redis_model.models import DataFile
from dl_file_uploader_lib.s3_model.models import S3DataSourcePreview
from dl_file_uploader_task_interface.tasks import ProcessExcelTask
from dl_task_processor.state import wait_task


LOGGER = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_parse_excel_task(
    task_processor_client,
    task_state,
    s3_client,
    redis_model_manager,
    s3_model_manager,
    uploaded_excel_id,
    reader_app,
):
    rmm = redis_model_manager
    df = await DataFile.get(manager=rmm, obj_id=uploaded_excel_id)
    assert df.status == FileProcessingStatus.in_progress

    task = await task_processor_client.schedule(
        ProcessExcelTask(
            file_id=uploaded_excel_id,
            tenant_id="common",
        )
    )
    result = await wait_task(task, task_state)
    await sleep(60)

    assert result[-1] == "success"

    df = await DataFile.get(manager=rmm, obj_id=uploaded_excel_id)
    assert df.id == uploaded_excel_id
    assert df.status == FileProcessingStatus.ready

    assert len(df.sources) == 3
    dsrc = df.sources[0]
    assert dsrc.status == FileProcessingStatus.ready
    assert dsrc.title == "data.xlsx – Orders"
    assert [sch.user_type for sch in dsrc.raw_schema] == [
        UserDataType.integer,
        UserDataType.string,
        UserDataType.genericdatetime,
        UserDataType.genericdatetime,
        UserDataType.string,
        UserDataType.string,
        UserDataType.string,
        UserDataType.string,
        UserDataType.string,
        UserDataType.string,
        UserDataType.string,
        UserDataType.string,
        UserDataType.string,
        UserDataType.string,
        UserDataType.string,
        UserDataType.string,
        UserDataType.string,
        UserDataType.string,
        UserDataType.float,
        UserDataType.integer,
        UserDataType.float,
        UserDataType.float,
        UserDataType.float,
        UserDataType.string,
        UserDataType.float,
    ]
    assert [sch.name for sch in dsrc.raw_schema] == [
        "row",
        "order_id",
        "order_date",
        "ship_date",
        "ship_mode",
        "customer_id",
        "customer_name",
        "segment",
        "city",
        "state",
        "global_area",
        "postal_code",
        "market",
        "region",
        "product_id",
        "category",
        "sub-category",
        "product_name",
        "sales",
        "quantity",
        "discount",
        "profit",
        "shipping_cost",
        "order_priority",
        "ammount",
    ]
    assert [sch.title for sch in dsrc.raw_schema] == [
        "Row",
        "Order ID",
        "Order Date",
        "Ship Date",
        "Ship Mode",
        "Customer ID",
        "Customer Name",
        "Segment",
        "City",
        "State",
        "Global Area",
        "Postal Code",
        "Market",
        "Region",
        "Product ID",
        "Category",
        "Sub-Category",
        "Product Name",
        "Sales",
        "Quantity",
        "Discount",
        "Profit",
        "Shipping Cost",
        "Order Priority",
        "Ammount",
    ]

    preview = await S3DataSourcePreview.get(manager=s3_model_manager, obj_id=dsrc.preview_id)
    assert preview.id == dsrc.preview_id
    assert preview.preview_data[0] == [
        "1",
        "MX-2014-143658",
        "2014-10-02 00:00:00",
        "2014-10-06 00:00:00",
        "Standard Class",
        "SC-20575",
        "Sonia Cooley",
        "Consumer",
        "Mexico City",
        "Distrito Federal",
        "Mexico",
        None,
        "LATAM",
        "North",
        "OFF-LA-10002782",
        "Office Supplies",
        "Labels",
        "Hon File Folder Labels, Adjustable",
        "13.08",
        "3",
        "0",
        "4.56",
        "1.033",
        "Medium",
        "39.24",
    ]


@pytest.mark.asyncio
async def test_parse_excel_with_one_row_task(
    task_processor_client,
    task_state,
    s3_client,
    redis_model_manager,
    uploaded_excel_with_one_row_id,
    reader_app,
):
    uploaded_excel_id = uploaded_excel_with_one_row_id
    rmm = redis_model_manager
    df = await DataFile.get(manager=rmm, obj_id=uploaded_excel_id)
    assert df.status == FileProcessingStatus.in_progress

    task = await task_processor_client.schedule(
        ProcessExcelTask(
            file_id=uploaded_excel_id,
            tenant_id="common",
        )
    )
    result = await wait_task(task, task_state)
    await sleep(60)

    assert result[-1] == "success"

    df = await DataFile.get(manager=rmm, obj_id=uploaded_excel_id)
    assert df.status == FileProcessingStatus.ready
    assert df.id == uploaded_excel_id
    for src in df.sources:
        assert src.status == FileProcessingStatus.failed


@pytest.mark.asyncio
async def test_parse_excel_non_string_header(
    task_processor_client,
    task_state,
    s3_client,
    s3_model_manager,
    redis_model_manager,
    uploaded_excel_no_header_id,
    reader_app,
):
    rmm = redis_model_manager
    df = await DataFile.get(manager=rmm, obj_id=uploaded_excel_no_header_id)
    assert df.status == FileProcessingStatus.in_progress

    task = await task_processor_client.schedule(
        ProcessExcelTask(
            file_id=uploaded_excel_no_header_id,
            tenant_id="common",
        )
    )
    result = await wait_task(task, task_state)
    await sleep(60)
    assert result[-1] == "success"

    df = await DataFile.get(manager=rmm, obj_id=uploaded_excel_no_header_id)
    assert df.id == uploaded_excel_no_header_id
    assert df.status == FileProcessingStatus.ready

    assert len(df.sources) == 1
    dsrc = df.sources[0]
    assert dsrc.status == FileProcessingStatus.ready
    assert dsrc.title == "no_header.xlsx – Sheet1"
    assert [sch.user_type for sch in dsrc.raw_schema] == [
        UserDataType.integer,
        UserDataType.integer,
        UserDataType.integer,
    ]
    assert [sch.name for sch in dsrc.raw_schema] == ["a", "b", "c"]
    assert [sch.title for sch in dsrc.raw_schema] == ["A", "B", "C"]

    preview = await S3DataSourcePreview.get(manager=s3_model_manager, obj_id=dsrc.preview_id)
    assert preview.id == dsrc.preview_id
    assert preview.preview_data[0] == ["1", "2", "3"]


@pytest.mark.asyncio
async def test_parse_invalid_excel(
    task_processor_client,
    task_state,
    s3_client,
    redis_model_manager,
    uploaded_invalid_excel_id,
    reader_app,
):
    rmm = redis_model_manager
    df = await DataFile.get(manager=rmm, obj_id=uploaded_invalid_excel_id)
    assert df.status == FileProcessingStatus.in_progress

    task = await task_processor_client.schedule(
        ProcessExcelTask(
            file_id=uploaded_invalid_excel_id,
            tenant_id="common",
        )
    )
    result = await wait_task(task, task_state)

    await sleep(60)
    assert result[-1] == "failed"

    df = await DataFile.get(manager=rmm, obj_id=uploaded_invalid_excel_id)
    assert df.status == FileProcessingStatus.failed
    for src in df.sources:
        assert src.status == FileProcessingStatus.failed
    assert df.error.code == ["FILE", "PARSE_FAILED", "INVALID_EXCEL"]
