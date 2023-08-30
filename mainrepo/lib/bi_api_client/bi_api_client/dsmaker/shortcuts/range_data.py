from typing import Any

from bi_api_client.dsmaker.api.data_api import HttpDataApiResponse
from bi_api_client.dsmaker.shortcuts.result_data import get_data_rows


def get_range_values(response: HttpDataApiResponse) -> tuple[Any, Any]:
    data_rows = get_data_rows(response)
    if response.api_version in ('v1', 'v1.5'):
        values = (data_rows[0][0], data_rows[1][0])
    elif response.api_version == 'v2':
        values = (data_rows[0][0], data_rows[0][1])
    else:
        raise ValueError(response.api_version)
    return values
