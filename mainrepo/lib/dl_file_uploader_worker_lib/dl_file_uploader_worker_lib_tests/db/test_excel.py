from asyncio import sleep
import logging

import pytest

from dl_constants.enums import (
    BIType,
    FileProcessingStatus,
)
from dl_file_uploader_lib.redis_model.models import (
    DataFile,
    DataSourcePreview,
)
from dl_file_uploader_task_interface.tasks import ProcessExcelTask
from dl_task_processor.state import wait_task


LOGGER = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_parse_excel_task(
    task_processor_client,
    task_state,
    s3_client,
    redis_model_manager,
    uploaded_excel_id,
    reader_app,
):
    rmm = redis_model_manager
    df = await DataFile.get(manager=rmm, obj_id=uploaded_excel_id)
    assert df.status == FileProcessingStatus.in_progress

    task = await task_processor_client.schedule(ProcessExcelTask(file_id=uploaded_excel_id))
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
        BIType.integer,
        BIType.string,
        BIType.genericdatetime,
        BIType.genericdatetime,
        BIType.string,
        BIType.string,
        BIType.string,
        BIType.string,
        BIType.string,
        BIType.string,
        BIType.string,
        BIType.string,
        BIType.string,
        BIType.string,
        BIType.string,
        BIType.string,
        BIType.string,
        BIType.string,
        BIType.float,
        BIType.integer,
        BIType.float,
        BIType.float,
        BIType.float,
        BIType.string,
        BIType.float,
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

    preview = await DataSourcePreview.get(manager=rmm, obj_id=dsrc.preview_id)
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
