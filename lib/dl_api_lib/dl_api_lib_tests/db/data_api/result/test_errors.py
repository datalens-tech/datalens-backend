from dl_api_client.dsmaker.api.data_api import SyncHttpDataApiV1
from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_commons.base_models import DLHeadersCommon
from dl_api_lib.app_settings import ControlApiAppSettings
from dl_core.us_manager.us_manager_sync import SyncUSManager
import pytest

from dl_api_client.dsmaker.primitives import (
    Dataset,
    RequestLegendItem,
    RequestLegendItemRef,
    ResultField,
    RoleSpec,
)
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_constants.enums import (
    AggregationFunction,
    CalcMode,
    FieldRole,
    QueryItemRefType,
    RawSQLLevel,
)


class TestResultErrors(DefaultApiTestBase):
    def test_empty_query(self, saved_dataset, data_api):
        result_resp = data_api.get_result(dataset=saved_dataset, fields=[], fail_ok=True)
        assert result_resp.status_code == 400
        assert result_resp.bi_status_code == "ERR.DS_API.EMPTY_QUERY"

    def test_get_nonexisting_field(self, saved_dataset, data_api):
        result_resp = data_api.get_result(dataset=saved_dataset, fields=[ResultField(title="unknown")], fail_ok=True)
        assert result_resp.status_code == 400
        assert result_resp.bi_status_code == "ERR.DS_API.FIELD.NOT_FOUND"

    @pytest.mark.parametrize("ref_type", (QueryItemRefType.id, QueryItemRefType.title))
    def test_incomplete_field_ref(self, saved_dataset, data_api, ref_type):
        error_msg = f"{{'fields': {{0: {{'ref': {{'{ref_type.value}': ['Missing data for required field.']}}}}}}}}"
        result_resp = data_api.get_result(
            dataset=saved_dataset,
            fields=[
                RequestLegendItem(
                    ref=RequestLegendItemRef(type=ref_type),
                    role_spec=RoleSpec(role=FieldRole.row),
                ),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == 400
        assert result_resp.bi_status_code == "ERR.DS_API"
        assert result_resp.json["message"] == error_msg

    def test_calcmode_formula_without_formula_field(self, saved_dataset, data_api):
        ds = saved_dataset
        title = "Not a formula"
        ds.result_schema[title] = ds.field(title=title, calc_mode=CalcMode.formula)

        result_resp = data_api.get_result(dataset=ds, fields=[ds.find_field(title)], fail_ok=True)
        assert result_resp.status_code == 400
        assert result_resp.bi_status_code == "ERR.DS_API.FORMULA.PARSE.UNEXPECTED_EOF.EMPTY_FORMULA"

    def test_validate_formula_recursion_error(self, saved_dataset, data_api):
        # Create large code that will cause RecursionError
        num = 1000

        code = ""
        for if_num in range(num):
            code += f'if([row_id] = {if_num}, "row_id_{if_num}", '
        code += '"row_id_max"'
        code += ")" * num

        ds = saved_dataset
        title = "row_id to string"
        ds.result_schema[title] = ds.field(
            title=title,
            calc_mode=CalcMode.formula,
            formula=code,
        )

        result_resp = data_api.get_result(dataset=ds, fields=[ds.find_field(title)], fail_ok=True)
        assert result_resp.status_code == 400
        assert result_resp.bi_status_code == "ERR.DS_API.FORMULA.PARSE.RECURSION"

    def test_filter_errors(self, saved_dataset, data_api):
        ds = saved_dataset
        ds.result_schema["New date"] = ds.field(formula="DATE(NOW())")  # just a date field
        ds.result_schema["New bool"] = ds.field(formula='"P" = "NP"')  # just a bool field

        # invalid value for date
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[ds.find_field(title="New date")],
            filters=[ds.find_field(title="New date").filter("EQ", values=["Office Supplies"])],
            fail_ok=True,
        )
        assert result_resp.status_code == 400
        assert result_resp.bi_status_code == "ERR.DS_API.FILTER.INVALID_VALUE"

        # a very special check for invalid bool values
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[ds.find_field(title="New date")],
            filters=[ds.result_schema["New bool"].filter("EQ", values=[""])],
            fail_ok=True,
        )
        assert result_resp.status_code == 400
        assert result_resp.bi_status_code == "ERR.DS_API.FILTER.INVALID_VALUE"

        # unknown field
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[ds.find_field(title="New date")],
            filters=[ds.field(title="Made-up field").filter("EQ", values=["whatever"])],
            fail_ok=True,
        )
        assert result_resp.status_code == 400
        assert result_resp.bi_status_code == "ERR.DS_API.FIELD.NOT_FOUND"

    @pytest.mark.parametrize(
        ("filter_name", "filter_values"),
        (
            ("eq", []),
            ("isnull", [None]),
        ),
    )
    def test_invalid_argument_count_filter_error(self, saved_dataset, data_api, filter_name, filter_values):
        ds = saved_dataset
        field = ds.result_schema[0]
        expected_count = 1 if filter_name == "eq" else 0
        error_msg = f"Invalid argument count for {filter_name}: expected {expected_count}, got {len(filter_values)}"

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[field],
            filters=[field.filter(filter_name.upper(), values=filter_values)],
            fail_ok=True,
        )
        assert result_resp.status_code == 400
        assert result_resp.bi_status_code == "ERR.DS_API.FILTER.ARGUMENT_COUNT_ERROR"
        assert result_resp.json["message"] == error_msg

    def test_invalid_group_by_configuration(self, saved_dataset, data_api):
        ds = saved_dataset
        ds.result_schema["Measure"] = ds.field(title="Measure", aggregation=AggregationFunction.sum)

        result_resp = data_api.get_result(
            dataset=saved_dataset,
            fields=[ds.find_field(title="Measure")],
            disable_group_by=True,
            fail_ok=True,
        )
        assert result_resp.status_code == 400
        assert result_resp.bi_status_code == "ERR.DS_API.INVALID_GROUP_BY_CONFIGURATION"
        assert result_resp.json["message"] == "Invalid parameter disable_group_by for dataset with measure fields"

    @pytest.mark.asyncio
    async def test_disallowed_dashsql(self, data_api_lowlevel_aiohttp_client, saved_connection_id):
        client = data_api_lowlevel_aiohttp_client
        conn_id = saved_connection_id
        req_data = {"sql_query": "select 1, 2, 3"}

        resp = await client.post(f"/api/v1/connections/{conn_id}/dashsql", json=req_data)
        resp_data = await resp.json()
        assert resp.status == 400
        assert resp_data["code"] == "ERR.DS_API.CONNECTION_CONFIG.DASHSQL_NOT_ALLOWED"

    def test_dataset_with_deleted_connection(self, saved_dataset, saved_connection_id, data_api, sync_us_manager):
        sync_us_manager.delete(sync_us_manager.get_by_id(saved_connection_id))
        result_resp = data_api.get_result(dataset=saved_dataset, fields=[saved_dataset.result_schema[0]], fail_ok=True)
        assert result_resp.status_code == 400
        assert result_resp.bi_status_code == "ERR.DS_API.REFERENCED_ENTRY_NOT_FOUND"
        assert result_resp.json["message"] == f"Referenced connection does not exist (connection id: {saved_connection_id})"

    def test_dataset_with_null_connection(
        self,
        saved_dataset: Dataset,
        control_api: SyncHttpDatasetApiV1,
        control_api_app_settings: ControlApiAppSettings,
        data_api: SyncHttpDataApiV1,
        sync_us_manager: SyncUSManager,
    ) -> None:
        # the only intended way to create such a dataset is via export-import, so let's create it that way
        export_import_headers = {
            DLHeadersCommon.US_MASTER_TOKEN.value: control_api_app_settings.US_MASTER_TOKEN,
        }
        export_req_data = {"id_mapping": {}}
        export_resp = control_api.export_dataset(dataset=saved_dataset, data=export_req_data, headers=export_import_headers)
        assert export_resp.status_code == 200, export_resp.json
        assert export_resp.json["dataset"]["sources"][0]["connection_id"] == None

        import_req_data: dict = {
            "id_mapping": {},
            "data": {"workbook_id": None, "dataset": export_resp.json["dataset"]},
        }
        import_resp = control_api.import_dataset(data=import_req_data, headers=export_import_headers)
        assert import_resp.status_code == 200, f"{import_resp.json} vs {export_resp.json}"

        ds = control_api.serial_adapter.load_dataset_from_response_body(Dataset(), export_resp.json)

        query = data_api.serial_adapter.make_req_data_get_result(
            dataset=None,
            fields=[ds.result_schema[0]],
        )
        headers = {
            "Content-Type": "application/json",
        }
        result_resp = data_api.get_response_for_dataset_result(
            dataset_id=import_resp.json["id"],
            raw_body=query,
            headers=headers,
        )

        assert result_resp.status_code == 400, result_resp.json
        assert result_resp.json["code"] == "ERR.DS_API.REFERENCED_ENTRY_NOT_FOUND"
        assert result_resp.json["message"] == "Referenced connection does not exist (connection id: empty)"

        control_api.delete_dataset(dataset_id=import_resp.json["id"])


class TestDashSQLErrors(DefaultApiTestBase):
    raw_sql_level = RawSQLLevel.dashsql

    @pytest.mark.asyncio
    async def test_invalid_param_value(self, data_api_lowlevel_aiohttp_client, saved_connection_id):
        client = data_api_lowlevel_aiohttp_client
        conn_id = saved_connection_id
        req_data = {
            "sql_query": r"SELECT {{date}}",
            "params": {
                "date": {
                    "type_name": "date",
                    "value": "Invalid date",
                },
            },
        }

        resp = await client.post(f"/api/v1/connections/{conn_id}/dashsql", json=req_data)
        resp_data = await resp.json()
        assert resp.status == 400
        assert resp_data["code"] == "ERR.DS_API.DASHSQL"
        assert resp_data["message"] == "Unsupported value for type 'date': 'Invalid date'"

    @pytest.mark.asyncio
    async def test_invalid_param_format(self, data_api_lowlevel_aiohttp_client, saved_connection_id):
        client = data_api_lowlevel_aiohttp_client
        conn_id = saved_connection_id
        req_data = {
            "sql_query": r"SELECT 'some_{{value}}'",
            "params": {
                "value": {
                    "type_name": "string",
                    "value": "value",
                },
            },
        }

        resp = await client.post(f"/api/v1/connections/{conn_id}/dashsql", json=req_data)
        resp_data = await resp.json()
        assert resp.status == 400
        assert resp_data["code"] == "ERR.DS_API.DB.WRONG_QUERY_PARAMETERIZATION"
        assert resp_data["message"] == "Wrong query parameterization. Parameter was not found"
