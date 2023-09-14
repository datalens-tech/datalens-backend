from http import HTTPStatus
from typing import (
    Any,
    Union,
)

from bi_api_client.dsmaker.api.data_api import (
    HttpDataApiResponse,
    SyncHttpDataApiV1,
    SyncHttpDataApiV2,
)
from bi_api_client.dsmaker.primitives import Dataset


def get_data_rows(response: HttpDataApiResponse) -> list[list[Any]]:
    if response.api_version == "v2":
        raw_data_rows = response.json["result_data"][0]["rows"]
        data_rows = [row["data"] for row in raw_data_rows]
    else:
        data_rows = response.json["result"]["data"]["Data"]
    return data_rows


def get_regular_result_data(
    ds: Dataset,
    data_api: Union[SyncHttpDataApiV1, SyncHttpDataApiV2],
    field_names: list[str],
) -> dict[str, list[str]]:
    field_names = sorted(set(field_names))  # Deduplicate fields
    result_resp = data_api.get_result(
        dataset=ds,
        fields=[ds.find_field(title=field_name) for field_name in field_names],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)

    return {field_name: [row[field_idx] for row in data_rows] for field_idx, field_name in enumerate(field_names)}
