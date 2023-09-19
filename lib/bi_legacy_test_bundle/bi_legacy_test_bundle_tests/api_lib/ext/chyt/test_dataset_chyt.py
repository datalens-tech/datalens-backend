""" ... """
# pylint: disable=redefined-outer-name

from __future__ import annotations

import os
from typing import ClassVar

import pytest

from bi_legacy_test_bundle_tests.api_lib.utils import get_result_schema
from dl_api_client.dsmaker.api.data_api import SyncHttpDataApiV1
from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib_testing.client import (
    FlaskSyncApiClient,
    TestClientConverterAiohttpToFlask,
    WrappedAioSyncApiClient,
)
from dl_constants.enums import CreateDSFrom

from bi_connector_chyt_internal.core.constants import (
    SOURCE_TYPE_CHYT_SUBSELECT,
    SOURCE_TYPE_CHYT_TABLE,
    SOURCE_TYPE_CHYT_TABLE_LIST,
    SOURCE_TYPE_CHYT_TABLE_RANGE,
    SOURCE_TYPE_CHYT_USER_AUTH_SUBSELECT,
    SOURCE_TYPE_CHYT_USER_AUTH_TABLE,
    SOURCE_TYPE_CHYT_USER_AUTH_TABLE_LIST,
    SOURCE_TYPE_CHYT_USER_AUTH_TABLE_RANGE,
)


# ####### Commons #######


@pytest.fixture(scope="function")
def api_v1_ch_over_yt_user_auth(client, public_ch_over_yt_user_auth_headers) -> SyncHttpDatasetApiV1:
    return SyncHttpDatasetApiV1(
        client=FlaskSyncApiClient(int_wclient=client),
        headers=public_ch_over_yt_user_auth_headers,
    )


@pytest.fixture(scope="function")
def data_api_v1_ch_over_yt_user_auth(
    loop, async_api_local_env_low_level_client, public_ch_over_yt_user_auth_headers
) -> SyncHttpDataApiV1:
    return SyncHttpDataApiV1(
        client=WrappedAioSyncApiClient(
            int_wrapped_client=TestClientConverterAiohttpToFlask(
                loop=loop,
                aio_client=async_api_local_env_low_level_client,
            ),
        ),
        headers=public_ch_over_yt_user_auth_headers,
    )


def _get_chyt_table_concat_cases(
    table_source_type,
    table_list_source_type,
    table_range_source_type,
    subselect_source_type,
):
    return (
        # name, source_kwargs
        (
            "00_single_table",
            dict(
                source_type=table_source_type,
                parameters=dict(
                    table_name="//home/yandexbi/datalens-back/bi_test_data/bi_225",
                ),
            ),
        ),
        # data: https://yql.yandex-team.ru/Operations/XdKNj2im9SVppzT6hU5mjZuVUqXf0SoUa4fj4IBTe60=
        # check: https://yql.yandex-team.ru/Operations/XdKQbAlcTrJgdZONZoznHFlirAScER885g5Q-gje604=
        (
            "01_table_list",
            dict(
                source_type=table_list_source_type,
                parameters=dict(  # Explicit table list, messy with whitespaces, with face URL
                    table_names="""
                //home/yandexbi/datalens-back/bi_test_data/chyt_table_func__bi_418/table04

                //home/yandexbi/datalens-back/bi_test_data/chyt_table_func__bi_418/table06
                https://yt.yandex-team.ru/hahn/navigation?path=//home/yandexbi/datalens-back/bi_test_data/chyt_table_func__bi_418/table07&offsetMode=row
                """,
                ),
            ),
        ),
        (
            "02_range_full_dir",
            dict(
                source_type=table_range_source_type,
                parameters=dict(  # Full
                    directory_path="//home/yandexbi/datalens-back/bi_test_data/chyt_table_func__bi_418",
                    range_from="",
                    range_to="",
                ),
            ),
        ),
        (
            "03_range_from",
            dict(
                source_type=table_range_source_type,
                parameters=dict(
                    directory_path="https://yt.yandex-team.ru/hahn/navigation?path=//home/yandexbi/datalens-back/bi_test_data/chyt_table_func__bi_418&",
                    range_from="table06",  # ['table06', 'table07']
                    range_to="",
                ),
            ),
        ),
        (
            "04_range_stuff",
            dict(
                source_type=table_range_source_type,
                parameters=dict(
                    directory_path="https://yt.yandex-team.ru/hahn/navigation?path=//home/yandexbi/datalens-back/bi_test_data/chyt_table_func__bi_418&",
                    range_from="table04z",  # does not have to match any specific table
                    range_to="table07",  # end is included
                ),
            ),
        ),
        (
            "05_subselect",
            dict(
                source_type=subselect_source_type,
                parameters=dict(
                    subsql='select *, 123 as extra from "//home/yandexbi/datalens-back/bi_test_data/bi_225" limit 10',
                ),
            ),
        ),
    )


def _get_chyt_table_with_large_row(table_source_type):
    return (
        # name, source_kwargs
        (
            "table_with_large_row",
            dict(
                source_type=table_source_type,
                parameters=dict(
                    table_name="//home/yandexbi/datalens-back/bi_test_data/table_with_large_row",
                ),
            ),
        ),
    )


# MAYBE TODO: same with `ch_over_yt_connection_id`
# (formerly: `pytest.mark.skip(reason='the test is yet to come')`)


def assert_is_subset(smaller, larger):
    larger = {key: val for key, val in larger.items() if key in smaller}
    assert smaller == larger


def _get_chyt_dataset(src_kwargs, request, client, api_v1, conn_id):
    ds = Dataset()
    ds.sources["source_1"] = ds.source(connection_id=conn_id, **src_kwargs)
    ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
    # User input should be kept as-is here:
    assert_is_subset(src_kwargs["parameters"], ds.sources["source_1"].parameters)
    ds = api_v1.apply_updates(dataset=ds).dataset
    assert_is_subset(src_kwargs["parameters"], ds.sources["source_1"].parameters)
    assert ds.sources["source_1"].source_type == src_kwargs["source_type"]
    ds_resp = api_v1.save_dataset(dataset=ds, preview=False)
    assert ds_resp.status_code == 200
    ds = ds_resp.dataset
    assert_is_subset(src_kwargs["parameters"], ds.sources["source_1"].parameters)

    def teardown(ds_id=ds.id):
        client.delete("/api/v1/datasets/{}".format(ds_id))

    request.addfinalizer(teardown)

    return ds


class BaseTestDatasetChyt:
    table_source_type: ClassVar[CreateDSFrom]
    table_list_source_type: ClassVar[CreateDSFrom]
    table_range_source_type: ClassVar[CreateDSFrom]
    subselect_source_type: ClassVar[CreateDSFrom]

    @pytest.mark.external_ipv6
    def test_dataset_create_chyt(self, dataset):
        assert dataset

    def test_dataset_result(self, client, data_api, dataset):
        result_schema = get_result_schema(client, dataset.id)
        # TODO?: all columns
        columns = [result_schema[0]["guid"]]
        response = data_api.get_response_for_dataset_result(
            dataset_id=dataset.id,
            raw_body={"columns": columns, "limit": 3},
        )
        assert response.status_code == 200

    def test_large_row_dataset(self, client, data_api, dataset_large_data):
        result_schema = get_result_schema(client, dataset_large_data.id)
        # TODO?: all columns
        columns = [result_schema[0]["guid"]]
        response = data_api.get_response_for_dataset_result(
            dataset_id=dataset_large_data.id,
            raw_body={"columns": columns, "limit": 3},
        )
        assert response.status_code == 400
        assert response.json["message"] == "Data source failed to respond correctly" " (too large row)."

    def test_invalid_tablelist(self, request, client, api, connection_id):
        src_kwargs = dict(
            source_type=self.table_list_source_type,
            parameters=dict(
                table_names="""
                //home/yandexbi/datalens-back/bi_test_data/chyt_table_func__bi_418/table04

                //home/yandexbi/datalens-back/bi_test_data/chyt_table_func__bi_418/table06
                https://yt.yandex-team.ru/hahn/navigation?path=//home/yandexbi/datalens-back/bi_test_data/chyt_table_func__bi_418/table07&offsetMode=row
                https://yt.yandex-team.ru/hahn/navigation?path=//home/yandexbi/datalens-back/bi_test_data/chyt_table_func__bi_418/table06&offsetMode=row
                """,
            ),
        )
        ds = Dataset()
        ds.sources["source_1"] = ds.source(connection_id=connection_id, **src_kwargs)
        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
        resp = api.apply_updates(dataset=ds, fail_ok=True)
        ds_errs = resp.dataset.component_errors.items
        assert len(ds_errs) == 1, ds_errs
        ds_err = ds_errs[0]
        assert ds_err.type.name == "data_source", ds_err
        assert len(ds_err.errors) == 1, ds_err
        ds_err_err = ds_err.errors[0]
        assert ds_err_err.code == "ERR.DS_API.SOURCE_CONFIG.TABLE_NAME_INVALID"
        assert "duplicates" in ds_err_err.message, "must state the reason"
        assert "/table06" in ds_err_err.message, "must list an example"

    def test_invalid_subsql(self, api, connection_id):
        src_kwargs = dict(
            source_type=self.subselect_source_type,
            parameters=dict(
                subsql="foo",
            ),
        )

        ds = Dataset()
        ds.sources["source_1"] = ds.source(connection_id=connection_id, **src_kwargs)
        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
        # User input should be kept as-is here:
        assert_is_subset(src_kwargs["parameters"], ds.sources["source_1"].parameters)
        ds = api.apply_updates(dataset=ds, fail_ok=True).dataset
        assert ds.component_errors.items[0].errors[0].details, "should have nonempty details"

    def test_connection_sources(self, client, connection_id):
        resp = client.get(f"/api/v1/connections/{connection_id}/info/sources")
        assert resp.status_code == 200, resp.json
        resp_data = resp.json
        assert len(resp_data["sources"]) in (0, 1), resp_data
        assert len(resp_data["freeform_sources"]) == 4, resp_data
        src_by_st = {item["source_type"]: item for item in resp_data["freeform_sources"] + resp_data["sources"]}
        # r_s_p: response_source_parameters, i.e. response_data__data_source__parameters
        src_tpl = src_by_st[self.table_source_type.name]
        assert "tab_title" in src_tpl
        assert isinstance(src_tpl["tab_title"], str)
        form = src_tpl["form"][0]
        assert form["name"] == "table_name"

    def test_special_fields_for_range(self, api, data_api, connection_id):
        dir_path = "//home/yandexbi/datalens-back/bi_test_data/chyt_table_func__bi_418"
        src_kwargs = dict(
            source_type=self.table_range_source_type,
            parameters=dict(  # Full
                directory_path=dir_path,
                range_from="",
                range_to="",
            ),
        )

        ds = Dataset()
        ds.sources["source_1"] = ds.source(connection_id=connection_id, **src_kwargs)
        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
        ds = api.apply_updates(dataset=ds).dataset
        column_sources = [field.source for field in ds.result_schema]

        special_keys = ("$table_path", "$table_name", "$table_index")
        for key in special_keys:
            assert key in column_sources
        ds_resp = api.save_dataset(ds)
        assert ds_resp.status_code == 200
        ds = ds_resp.dataset

        response = data_api.get_response_for_dataset_result(
            dataset_id=ds.id,
            raw_body={
                "columns": special_keys,
                "order_by": [
                    {
                        "column": "$table_index",
                        "direction": "ASC",
                    }
                ],
                "limit": 3,
            },
        )
        assert response.status_code == 200
        data = response.json["result"]["data"]["Data"]
        for idx, item in enumerate(data):
            assert os.path.dirname(item[0]) == dir_path
            assert os.path.basename(item[0]) == item[1]  # last part of table_path == table_name
            assert int(item[2]) == idx


class TestDatasetChyt(BaseTestDatasetChyt):
    table_source_type = SOURCE_TYPE_CHYT_TABLE
    table_list_source_type = SOURCE_TYPE_CHYT_TABLE_LIST
    table_range_source_type = SOURCE_TYPE_CHYT_TABLE_RANGE
    subselect_source_type = SOURCE_TYPE_CHYT_SUBSELECT

    TABLE_CONCAT_CASES = _get_chyt_table_concat_cases(
        table_source_type=table_source_type,
        table_list_source_type=table_list_source_type,
        table_range_source_type=table_range_source_type,
        subselect_source_type=subselect_source_type,
    )

    TABLE_WITH_LARGE_ROW = _get_chyt_table_with_large_row(table_source_type=table_source_type)

    @pytest.fixture(scope="function")
    def api(self, api_v1) -> SyncHttpDatasetApiV1:
        return api_v1

    @pytest.fixture(scope="function")
    def data_api(self, data_api_v1) -> SyncHttpDataApiV1:
        return data_api_v1

    @pytest.fixture(params=TABLE_CONCAT_CASES, ids=[item[0] for item in TABLE_CONCAT_CASES])
    def dataset(self, request, client, api, connection_id):
        _, src_kwargs = request.param
        return _get_chyt_dataset(src_kwargs, request, client, api, connection_id)

    @pytest.fixture(params=TABLE_WITH_LARGE_ROW, ids=[item[0] for item in TABLE_WITH_LARGE_ROW])
    def dataset_large_data(self, request, client, api, connection_id):
        _, src_kwargs = request.param
        return _get_chyt_dataset(src_kwargs, request, client, api, connection_id)

    @pytest.fixture(scope="function")
    def connection_id(self, public_ch_over_yt_connection_id):
        return public_ch_over_yt_connection_id


class TestDatasetChytUserAuth(BaseTestDatasetChyt):
    table_source_type = SOURCE_TYPE_CHYT_USER_AUTH_TABLE
    table_list_source_type = SOURCE_TYPE_CHYT_USER_AUTH_TABLE_LIST
    table_range_source_type = SOURCE_TYPE_CHYT_USER_AUTH_TABLE_RANGE
    subselect_source_type = SOURCE_TYPE_CHYT_USER_AUTH_SUBSELECT

    TABLE_CONCAT_CASES = _get_chyt_table_concat_cases(
        table_source_type=table_source_type,
        table_list_source_type=table_list_source_type,
        table_range_source_type=table_range_source_type,
        subselect_source_type=subselect_source_type,
    )

    TABLE_WITH_LARGE_ROW = _get_chyt_table_with_large_row(table_source_type=table_source_type)

    @pytest.fixture(scope="function")
    def api(self, client, public_ch_over_yt_user_auth_headers) -> SyncHttpDatasetApiV1:
        return SyncHttpDatasetApiV1(
            client=FlaskSyncApiClient(int_wclient=client), headers=public_ch_over_yt_user_auth_headers
        )

    @pytest.fixture(scope="function")
    def data_api(
        self, loop, async_api_local_env_low_level_client, public_ch_over_yt_user_auth_headers
    ) -> SyncHttpDataApiV1:
        return SyncHttpDataApiV1(
            client=WrappedAioSyncApiClient(
                int_wrapped_client=TestClientConverterAiohttpToFlask(
                    loop=loop,
                    aio_client=async_api_local_env_low_level_client,
                ),
            ),
            headers=public_ch_over_yt_user_auth_headers,
        )

    @pytest.fixture(params=TABLE_CONCAT_CASES, ids=[item[0] for item in TABLE_CONCAT_CASES])
    def dataset(self, request, client, api, connection_id):
        _, src_kwargs = request.param
        return _get_chyt_dataset(src_kwargs, request, client, api, connection_id)

    @pytest.fixture(params=TABLE_WITH_LARGE_ROW, ids=[item[0] for item in TABLE_WITH_LARGE_ROW])
    def dataset_large_data(self, request, client, api, connection_id):
        _, src_kwargs = request.param
        return _get_chyt_dataset(src_kwargs, request, client, api, connection_id)

    @pytest.fixture(scope="function")
    def connection_id(self, public_ch_over_yt_user_auth_connection_id):
        return public_ch_over_yt_user_auth_connection_id
