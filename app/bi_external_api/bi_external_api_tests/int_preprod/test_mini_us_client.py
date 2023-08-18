import pytest

from bi_testing.utils import skip_outside_devhost


@skip_outside_devhost
@pytest.mark.asyncio
async def test_us_cli_get_folder_content(bi_ext_api_int_preprod_int_api_clients):
    us_cli = bi_ext_api_int_preprod_int_api_clients.us

    entries = await us_cli.get_folder_content("ext_api_tests")
    assert entries is not None
