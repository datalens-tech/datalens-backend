from typing import Optional

from dl_api_client.dsmaker.shortcuts.result_data import get_data_rows
from dl_api_lib_testing.api_base import DefaultApiTestBase


class TestResult(DefaultApiTestBase):
    def test_sorted_result(self, saved_dataset, data_api):
        ds = saved_dataset
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="discount"),
                ds.find_field(title="city"),
            ],
            order_by=[
                ds.find_field(title="discount").desc,
            ],
        )

        assert result_resp.status_code == 200
        data = get_data_rows(result_resp)
        assert all(data[i][0] <= data[i - 1][0] for i in range(1, len(data)))

    def test_result_with_limit(self, saved_dataset, data_api):
        ds = saved_dataset
        limit = 3
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="discount"),
                ds.find_field(title="city"),
            ],
            limit=limit,
        )

        assert result_resp.status_code == 200
        data = get_data_rows(result_resp)
        assert len(data) == limit

    def test_result_with_offset(self, saved_dataset, data_api):
        ds = saved_dataset
        limit_ = 10
        offset_ = 5

        def get_data(limit: Optional[int] = None, offset: Optional[int] = None) -> list[list]:
            result_resp = data_api.get_result(
                dataset=ds,
                fields=[
                    ds.find_field(title="discount"),
                    ds.find_field(title="city"),
                ],
                order_by=[
                    ds.find_field(title="discount").desc,
                ],
                limit=limit,
                offset=offset,
            )
            assert result_resp.status_code == 200
            return get_data_rows(result_resp)

        offset_data = get_data(limit=limit_, offset=offset_)
        total_data = get_data(limit=limit_ + offset_)
        assert offset_data == total_data[offset_ : limit_ + offset_]

    def test_result_with_duplicated_fields(self, saved_dataset, data_api):
        ds = saved_dataset
        result_resp = data_api.get_result(dataset=ds, fields=[field for field in ds.result_schema] * 2)
        assert result_resp.status_code == 200
        assert get_data_rows(result_resp)

    def test_result_with_disabled_group_by(self, saved_dataset, data_api):
        ds = saved_dataset
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[ds.find_field(title="city")],
            disable_group_by=True,
        )

        assert result_resp.status_code == 200
        data = get_data_rows(result_resp)
        data_flattened = [row[0] for row in data]
        assert len(data_flattened) > len(set(data_flattened))

        query = result_resp.json["blocks"][0]["query"]
        assert "GROUP BY" not in query
