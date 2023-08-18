import json
from copy import deepcopy
from typing import final, Any, Optional, ClassVar

import attr
import grpc
import pytest

from bi_external_api.grpc_proxy.client import GrpcClientCtx, VisualizationV1Client
from bi_external_api.testing_dicts_builders.chart import ChartJSONBuilderSingleDataset
from bi_external_api.testing_dicts_builders.dataset import SampleSuperStoreLightJSONBuilder, BaseDatasetJSONBuilder
from bi_external_api.testing_dicts_builders.reference_wb import build_reference_workbook
from bi_external_api.testing_no_deps import DomainScene
from bi_testing.factories import AbstractAsyncTestResourceFactory


def assert_operation_status(
        op: dict[str, Any],
        status: Optional[str] = None
) -> None:
    expected_status = status if status is not None else "STATUS_DONE"
    assert op["status"] == expected_status
    assert "error" not in op


@attr.s(frozen=True, auto_attribs=True)
class WorkbookAsyncFactory(AbstractAsyncTestResourceFactory[str, str]):
    grpc_client: VisualizationV1Client
    project_id: str
    preserve_workbook: bool

    async def _create_resource(
            self,
            resource_request: str
    ) -> str:
        create_wb_result = await self.grpc_client.create_workbook(wb_title=resource_request, project_id=self.project_id)
        wb_id = create_wb_result["resourceId"]
        return wb_id

    async def _close_resource(
            self,
            resource: str
    ) -> None:
        if not self.preserve_workbook:
            wb_delete_result = await self.grpc_client.delete_workbook(resource)
            assert_operation_status(wb_delete_result)


class GrpcAcceptanceScenarioDC:
    ALLOW_EXC_MSG_AND_STACK_TRACE_IN_ERRORS: ClassVar[bool] = False
    CHECK_GRPC_LOGS: ClassVar[bool] = False

    @pytest.fixture()
    def grpc_client_ctx(self) -> GrpcClientCtx:
        raise NotImplementedError()

    @pytest.fixture()
    def grpc_client_ctx_disposable_sa(self) -> GrpcClientCtx:
        raise NotImplementedError()

    @pytest.fixture()
    def workbook_title(self) -> str:
        raise NotImplementedError()

    @pytest.fixture()
    def project_id(self) -> str:
        raise NotImplementedError()

    @final
    @pytest.fixture()
    async def grpc_client(self, grpc_client_ctx) -> VisualizationV1Client:
        client = VisualizationV1Client(grpc_client_ctx)
        is_healthy = await grpc_client_ctx.is_alive()
        assert is_healthy
        return client

    @final
    @pytest.fixture()
    async def grpc_client_disposable_sa(self, grpc_client_ctx_disposable_sa) -> VisualizationV1Client:
        client = VisualizationV1Client(grpc_client_ctx_disposable_sa)
        is_healthy = await grpc_client_ctx_disposable_sa.is_alive()
        assert is_healthy
        return client

    def get_mark(self, request: pytest.FixtureRequest, name: str) -> Optional[pytest.Mark]:
        for mark in request.node.iter_markers():
            if mark.name == name:
                return mark
        return None

    @final
    @pytest.fixture(scope="function")
    async def workbook_id_factory(
            self,
            request: pytest.FixtureRequest,
            project_id: str,
            grpc_client: VisualizationV1Client,
    ) -> WorkbookAsyncFactory:
        async with WorkbookAsyncFactory(
                grpc_client=grpc_client,
                project_id=project_id,
                preserve_workbook=self.get_mark(request, "ea_preserve_workbook") is not None
        ) as f:
            yield f

    @final
    @pytest.fixture()
    async def workbook_id(
            self,
            workbook_title: str,
            workbook_id_factory: WorkbookAsyncFactory
    ) -> str:
        return await workbook_id_factory.create(workbook_title)

    @pytest.fixture()
    def domain_scene(self) -> DomainScene:
        raise NotImplementedError()

    @pytest.fixture()
    async def ch_conn_name(
            self,
            domain_scene: DomainScene,
            workbook_id: str,
            grpc_client: VisualizationV1Client,
    ) -> str:
        conn_name = "ch_conn"

        initial_conn_data = domain_scene.get_connection_params()
        conn_secret = domain_scene.get_connection_secret()["secret"]

        create_conn_result = await grpc_client.create_connection(
            wb_id=workbook_id,
            name=conn_name,
            plain_secret=conn_secret,
            connection_params=initial_conn_data,
        )
        assert_operation_status(create_conn_result)
        return conn_name

    @classmethod
    def clear_volatile_fields_in_err_response(cls, err: dict[str, Any]) -> dict[str, Any]:
        assert set(err.keys()) == {
            'message',
            'common_errors',
            'entry_errors',
            'partial_workbook',
            'request_id',
        }
        err = deepcopy(err)
        assert err['request_id']

        for single_err in err["common_errors"]:
            exc_msg = single_err.pop("exc_message")
            trace = single_err.pop("stacktrace")

            if not cls.ALLOW_EXC_MSG_AND_STACK_TRACE_IN_ERRORS:
                assert exc_msg is None and trace is None, f"Got exc message or stacktrace in API error: {err}"

        for entry_err in err["entry_errors"]:
            for single_err in entry_err["errors"]:
                exc_msg = single_err.pop("exc_message")
                trace = single_err.pop("stacktrace")

                if not cls.ALLOW_EXC_MSG_AND_STACK_TRACE_IN_ERRORS:
                    assert exc_msg is None and trace is None, f"Got exc message or stacktrace  in API error: {err}"

        return err

    @pytest.mark.asyncio
    async def test_workbook_create_delete(
            self,
            workbook_title: str,
            project_id: str,
            grpc_client: VisualizationV1Client,
    ):
        create_wb_result = await grpc_client.create_workbook(wb_title=workbook_title, project_id=project_id)
        assert_operation_status(create_wb_result)

        wb_id = create_wb_result["resourceId"]

        try:
            # Check that workbook was created & it is empty
            empty_workbook = dict(
                datasets=[],
                charts=[],
                dashboards=[],
            )
            get_wb_result = await grpc_client.get_workbook(wb_id)
            assert get_wb_result == dict(workbook=dict(config=empty_workbook))
        finally:
            del_wb_result = await grpc_client.delete_workbook(wb_id)

        assert_operation_status(del_wb_result)

        # Ensure workbook was deleted
        with pytest.raises(BaseException) as err_info:
            await grpc_client.get_workbook_raw(wb_id)
        issubclass(err_info.type, grpc.RpcError)
        assert err_info.value.code() == grpc.StatusCode.NOT_FOUND
        details_json = json.loads(err_info.value.details())
        assert details_json["request_id"], f"No request ID in details: {details_json}"
        assert details_json["message"] == "Workbook was not found"

    @pytest.mark.parametrize("bad_wb_id", [
        "12345678901234",
        "#ololo",
        "__13__",
        "../../",
    ])
    @pytest.mark.asyncio
    async def test_bad_workbook_id_format(
            self,
            bad_wb_id: str,
            grpc_client: VisualizationV1Client,
    ):
        # Ensure workbook was deleted
        with pytest.raises(BaseException) as err_info:
            await grpc_client.get_workbook(bad_wb_id)
        issubclass(err_info.type, grpc.RpcError)
        assert err_info.value.code() == grpc.StatusCode.NOT_FOUND, err_info.value.details()
        details_json = json.loads(err_info.value.details())
        assert details_json["message"] == "Workbook was not found"

    @pytest.mark.asyncio
    async def test_connection_create_modify_read_delete(
            self,
            grpc_client: VisualizationV1Client,
            workbook_id: str,
            domain_scene: DomainScene,
    ):
        conn_name = "ch_conn"

        initial_conn_data = domain_scene.get_connection_params(overrides=dict(port=8080))
        conn_secret = domain_scene.get_connection_secret()["secret"]

        create_conn_result = await grpc_client.create_connection(
            wb_id=workbook_id,
            name=conn_name,
            plain_secret=conn_secret,
            connection_params=initial_conn_data,
        )
        assert_operation_status(create_conn_result)

        conn_read_result = await grpc_client.get_connection(wb_id=workbook_id, name=conn_name)
        assert conn_read_result == {
            "connection": {
                "config": {
                    "connection": initial_conn_data,
                    "name": conn_name,
                }
            }
        }

        # Modifying connection
        modified_conn_data = domain_scene.get_connection_params()

        modify_conn_result = await grpc_client.update_connection(
            wb_id=workbook_id,
            name=conn_name,
            plain_secret=conn_secret,
            connection_params=modified_conn_data,
        )
        assert_operation_status(modify_conn_result)

        # Ensure that connection was really modified
        conn_read_result = await grpc_client.get_connection(wb_id=workbook_id, name=conn_name)
        assert conn_read_result == {
            "connection": {
                "config": {
                    "connection": modified_conn_data,
                    "name": conn_name,
                }
            }
        }

        # Delete connection
        delete_conn_result = await grpc_client.delete_connection(wb_id=workbook_id, name=conn_name)
        assert_operation_status(delete_conn_result)

        # Ensure that connection is not available anymore
        with pytest.raises(BaseException) as err_info:
            await grpc_client.get_connection(workbook_id, conn_name)
        issubclass(err_info.type, grpc.RpcError)
        details_json = json.loads(err_info.value.details())

        assert err_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT

        assert details_json['message'] == 'Connection get error'
        assert 'not found in workbook' in details_json['common_errors'][0]['exc_message']
        assert details_json['request_id']

    @pytest.mark.asyncio
    async def test_advise_dataset_fields(
            self,
            grpc_client: VisualizationV1Client,
            ch_conn_name: str,
            workbook_id: str,
    ):
        base_ds_builder = SampleSuperStoreLightJSONBuilder(ch_conn_name)

        ds_wo_fields = base_ds_builder.build()
        advise_result = await grpc_client.advise_dataset_fields(
            wb_id=workbook_id,
            connection_name=ch_conn_name,
            partial_dataset=ds_wo_fields
        )

        expected_fields = base_ds_builder.do_add_default_fields().build()["fields"]
        expected_fields = [{**f, "title": f['id']} for f in expected_fields]
        assert len(expected_fields) != 0

        assert advise_result["dataset"]["config"]["fields"] == expected_fields

    @pytest.mark.asyncio
    async def test_save_complex_workbook(
            self,
            grpc_client: VisualizationV1Client,
            workbook_id: str,
            domain_scene: DomainScene,
            ch_conn_name: str,
    ):
        # Update wb with empty data
        empty_workbook_config = dict(
            datasets=[],
            charts=[],
            dashboards=[],
        )
        update_wb_result = await grpc_client.update_workbook(workbook_id, workbook=empty_workbook_config)
        assert_operation_status(update_wb_result)

        # Write complex WB example
        update_wb_result = await grpc_client.update_workbook(
            workbook_id,
            workbook=domain_scene.get_complex_workbook_dict(
                conn_name=ch_conn_name,
                fill_defaults=False,
            )
        )
        assert_operation_status(update_wb_result)

        get_complex_wb_resp = await grpc_client.get_workbook(workbook_id)
        read_wb_config = get_complex_wb_resp["workbook"]["config"]
        assert read_wb_config == domain_scene.get_complex_workbook_dict(
            conn_name=ch_conn_name,
            fill_defaults=True,
        )

        # Cleaning up workbook
        update_wb_result = await grpc_client.update_workbook(workbook_id, workbook=empty_workbook_config)
        assert_operation_status(update_wb_result)

        get_wb_resp_after_cleanup = await grpc_client.get_workbook(workbook_id)
        assert get_wb_resp_after_cleanup == dict(workbook=dict(config=empty_workbook_config))

    @pytest.mark.asyncio
    @pytest.mark.ea_preserve_workbook
    async def test_save_reference_workbook(
            self,
            grpc_client: VisualizationV1Client,
            workbook_id: str,
            ch_conn_name: str,
    ):
        wb_config = build_reference_workbook(ch_conn_name)

        resp = await grpc_client.update_workbook(wb_id=workbook_id, workbook=wb_config)
        assert_operation_status(resp)

    # TODO FIX: Check bad field ID in different sections (coloring, etc.)
    @pytest.mark.parametrize("chart_builder,errors", [
        [
            ChartJSONBuilderSingleDataset().with_visualization({
                "kind": "indicator",
                "field": {"source": {"kind": "ref", "id": "unk_field"}},
            }),
            [dict(
                message="Dataset field not found: id='unk_field'",
                path="visualization.field",
            )],
        ],
        [
            ChartJSONBuilderSingleDataset().with_visualization({
                "kind": "indicator",
                "field": {"source": {"kind": "ref", "id": "my_field"}},
            }).add_formula_field("my_field", cast="float", formula="sum([trololo])"),
            [dict(
                message="Unknown field found in formula: trololo, code: ERR.DS_API.FORMULA.UNKNOWN_FIELD_IN_FORMULA",
                path="field.my_field",
            )],
        ],
        # Check that title reference in ID formula triggers an error BI-4542
        [
            ChartJSONBuilderSingleDataset().with_visualization({
                "kind": "indicator",
                "field": {"source": {"kind": "ref", "id": "my_field"}},
            }).add_formula_field("my_field", cast="float", formula="sum([The Sales])"),
            [
                dict(
                    message="Dataset field not found: next fields in ID formula not found in dataset: The Sales",
                    path="ad_hoc_fields.my_field",
                ),
                dict(
                    message="Dataset field not found: id='my_field'",
                    path='visualization.field'
                ),
            ],
        ],
    ], ids=["unknown_field_indicator", "unknown_field_id_in_ad_hoc", "title_in_ad_hoc_formula_instead_of_id"])
    @pytest.mark.asyncio
    async def test_err_unknown_field_in_chart(
            self,
            grpc_client: VisualizationV1Client,
            workbook_id: str,
            ch_conn_name: str,
            chart_builder: ChartJSONBuilderSingleDataset,
            errors: list[dict[str, str]],
    ):
        ds_name = "ds_1"
        ch_name = "ch_1"

        workbook = dict(
            datasets=[SampleSuperStoreLightJSONBuilder(ch_conn_name, add_default_fields=True).build_instance(ds_name)],
            dashboards=[],
            charts=[chart_builder.with_ds_name(ds_name).build_instance(ch_name)]
        )
        with pytest.raises(grpc.RpcError) as err_info:
            await grpc_client.update_workbook(workbook_id, workbook)

        details = json.loads(err_info.value.details())
        assert self.clear_volatile_fields_in_err_response(details)["entry_errors"] == [dict(
            name=ch_name,
            errors=errors,
        )]

    @pytest.mark.parametrize("dataset_builder,errors", [
        [
            SampleSuperStoreLightJSONBuilder().add_field(
                BaseDatasetJSONBuilder.field_id_formula("sum([trololo])", cast="float", id="sum_trololo"),
            ),
            [dict(
                message="Dataset field not found: next fields in ID formula not found in dataset: trololo",
                path="fields.sum_trololo",
            )],
            # After removing crunch BI-4542 should be like this
            # [dict(
            #     message="Unknown field found in formula: trololo, code: ERR.DS_API.FORMULA.UNKNOWN_FIELD_IN_FORMULA",
            #     path="field.sum_trololo",
            # )],
        ],
        [
            SampleSuperStoreLightJSONBuilder(add_default_fields=True).add_field(
                BaseDatasetJSONBuilder.field_id_formula("sum([The Sales])", cast="float", id="sum_sales"),
            ),
            # Check that title reference in ID formula triggers an error BI-4542
            [dict(
                message="Dataset field not found: next fields in ID formula not found in dataset: The Sales",
                path="fields.sum_sales",
            )],
        ],
    ], ids=["really_non_existing_id_in_formula", "title_ref_in_id_formula"])
    @pytest.mark.asyncio
    async def test_err_in_dataset(
            self,
            grpc_client: VisualizationV1Client,
            workbook_id: str,
            ch_conn_name: str,
            dataset_builder: SampleSuperStoreLightJSONBuilder,
            errors: list[dict[str, str]],
    ):
        ds_name = "ds_1"

        workbook = dict(
            datasets=[dataset_builder.with_conn_name(ch_conn_name).build_instance(ds_name)],
            dashboards=[],
            charts=[]
        )
        with pytest.raises(grpc.RpcError) as err_info:
            await grpc_client.update_workbook(workbook_id, workbook)

        details = json.loads(err_info.value.details())
        assert self.clear_volatile_fields_in_err_response(details)["entry_errors"] == [dict(
            name=ds_name,
            errors=errors,
        )]

    @pytest.mark.asyncio
    async def test_proxy_errors(
            self,
            grpc_client: VisualizationV1Client,
            project_id: str,
            workbook_title: str,
            workbook_id: str
    ):
        # workbook with ID `workbook_id` and title `workbook_title`
        #  already created by fixtures
        # try to create wb with the same title to cause an exception
        with pytest.raises(BaseException) as err_info:
            await grpc_client.create_workbook(wb_title=workbook_title, project_id=project_id)

        issubclass(err_info.type, grpc.RpcError)
        details = json.loads(err_info.value.details())
        assert details

    @pytest.mark.asyncio
    async def test_create_conn_missing_secret_to_err(
            self,
            caplog,
            grpc_client: VisualizationV1Client,
            project_id: str,
            workbook_title: str,
            workbook_id: str,
            domain_scene: DomainScene,
    ):
        conn_name = "ch_conn"

        initial_conn_data = domain_scene.get_connection_params(overrides=dict(port=8080))

        with pytest.raises(grpc.RpcError) as err_info:
            await grpc_client.create_connection(
                wb_id=workbook_id,
                name=conn_name,
                plain_secret=None,
                connection_params=initial_conn_data,
            )

        details = json.loads(err_info.value.details())

        assert details
        assert "No secret for connection" in details['common_errors'][0]['exc_message']

        if self.CHECK_GRPC_LOGS:
            assert "Traceback" in caplog.text
            assert "No secret for connection" in caplog.text

    @pytest.mark.asyncio
    async def test_proxy_access_denied_error(
            self,
            grpc_client: VisualizationV1Client,
            grpc_client_disposable_sa: VisualizationV1Client,
            project_id: str,
            workbook_title: str,
            workbook_id: str
    ):
        await grpc_client.get_workbook(workbook_id)
        with pytest.raises(grpc.RpcError) as err_info:
            await grpc_client_disposable_sa.get_workbook(workbook_id)

        assert err_info.value.code() == grpc.StatusCode.PERMISSION_DENIED

    @final
    @pytest.fixture()
    async def test_list_workbooks_workbook_title(self):
        return 'test_list_workbooks_title'

    @final
    @pytest.fixture()
    async def test_list_workbooks_workbook_id(
            self,
            test_list_workbooks_workbook_title: str,
            workbook_id_factory: WorkbookAsyncFactory
    ) -> str:
        return await workbook_id_factory.create(test_list_workbooks_workbook_title)

    @pytest.mark.asyncio
    async def test_list_workbooks(
            self,
            grpc_client: VisualizationV1Client,
            project_id: str,
            test_list_workbooks_workbook_id: str,
            test_list_workbooks_workbook_title: str
    ):
        resp = await grpc_client.list_workbooks(project_id)
        wb_titles = {wb['title'] for wb in resp['workbooks']}
        assert test_list_workbooks_workbook_title in wb_titles
