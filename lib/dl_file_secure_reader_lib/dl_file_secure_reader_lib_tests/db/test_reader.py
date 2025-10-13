import aiohttp
from aiohttp.pytest_plugin import TestClient
import pytest


@pytest.mark.asyncio
async def test_reader(web_app: TestClient, excel_data: bytes) -> None:
    with aiohttp.MultipartWriter() as mpwriter:
        part = mpwriter.append(excel_data)
        part.set_content_disposition("form-data", name="file")
        resp = await web_app.post("/reader/excel", data=mpwriter)

    assert resp.status == 200
    result = await resp.json()
    assert len(result) == 3
    spreadsheet = result[2]
    assert spreadsheet["sheetname"] == "People"
    assert spreadsheet["data"][0] == [
        {"value": "Person", "data_type": "s"},
        {"value": "Region", "data_type": "s"},
    ]


@pytest.mark.asyncio
async def test_reader_yadocs_corrupted_patch(
    web_app: TestClient, yadocs_corrupted_excel_data: bytes, caplog: pytest.LogCaptureFixture
) -> None:
    with aiohttp.MultipartWriter() as mpwriter:
        part = mpwriter.append(yadocs_corrupted_excel_data)
        part.set_content_disposition("form-data", name="file")
        with caplog.at_level("DEBUG"):
            resp = await web_app.post("/reader/excel", data=mpwriter)

    assert resp.status == 200
    result = await resp.json()
    assert len(result) == 4

    # Check that patching was applied
    assert any("Patching horizontal alignment value start to left" in record.message for record in caplog.records)
