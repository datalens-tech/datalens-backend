from copy import deepcopy
from typing import (
    Any,
    Optional,
)

import attr
import requests

from bi_external_api.testing_dicts_builders.chart import ChartJSONBuilderSingleDataset
from bi_external_api.testing_dicts_builders.dash import DashJSONBuilderSingleTab
from bi_external_api.testing_dicts_builders.dataset import SampleSuperStoreLightJSONBuilder
from dl_api_commons.base_models import AuthData


@attr.s(kw_only=True)
class SimpleRequest:
    url: str = attr.ib()
    data_json: Optional[dict[str, Any]] = attr.ib(default=None)
    method: str = attr.ib(default="post")


@attr.s(auto_attribs=True)
class SimpleResponse:
    status_code: int
    text: Optional[str] = None
    json_resp: Optional[dict[str, Any]] = None


@attr.s(auto_attribs=True)
class SimpleRequestExecutor:
    auth_data: AuthData
    project_id: str
    headers: dict[str, Any]

    @property
    def auth_headers(self) -> dict[str, str]:
        return {dl_header.value: value for dl_header, value in self.auth_data.get_headers().items()}

    def make_request(self, req: SimpleRequest) -> SimpleResponse:
        headers = dict(self.headers.items())  # type: ignore
        headers.update(self.auth_headers)

        raw = requests.request(
            method=req.method,
            url=req.url,
            headers=headers,
            json=req.data_json,
        )
        text = json_resp = None
        try:
            text = raw.text
        except Exception:
            pass

        try:
            json_resp = raw.json()
        except Exception:  # noqa
            pass

        return SimpleResponse(
            status_code=raw.status_code,
            text=text,
            json_resp=json_resp,
        )


@attr.s(auto_attribs=True)
class DomainScene:
    ch_connection_data: dict[str, Any] = attr.ib()
    ch_connection_secret: dict[str, Any] = attr.ib()

    SAMPLE_SQL = """
SELECT * FROM format(CSVWithNamesAndTypes,
$$"category","customer_id","date","order_id","postal_code","profit","region","sales","segment","sub_category"
"String","String","Date","String","String","Float32","String","Float32","String","String"
Office Supplies,JD-15895,2022-01-01,CA-2022-143805,23223,694.5,South,2104.6,Corporate,Appliances
$$)
"""

    def get_connection_params(self, overrides: Optional[dict] = None) -> dict:
        effective_overrides: dict = overrides if overrides else {}
        conn_params = deepcopy(self.ch_connection_data)
        conn_params.update(effective_overrides)
        return conn_params

    def get_connection_secret(self) -> dict[str, Any]:
        return deepcopy(self.ch_connection_secret)

    def get_complex_workbook_dict(self, conn_name: str, *, fill_defaults: bool) -> dict[str, Any]:
        ds_name = "ds_sales"

        ds_builder = SampleSuperStoreLightJSONBuilder(conn_name=conn_name)
        ds_builder = ds_builder.do_add_default_fields().with_fill_defaults(fill_defaults)

        ds_instance = ds_builder.build_instance(ds_name)
        ds = ds_instance["dataset"]

        base_chart_builder = ChartJSONBuilderSingleDataset(source_dataset=ds).with_ds_name(ds_name)

        chart_instances = [
            base_chart_builder.with_fill_defaults(fill_defaults)
            .with_visualization(
                {
                    "kind": "indicator",
                    "field": {
                        "source": {
                            "kind": "ref",
                            "id": base_chart_builder.generate_agg_field_id(ds_field["id"], agg="sum"),
                            **({"dataset_name": ds_name} if fill_defaults else {}),
                        }
                    },
                }
            )
            .add_aggregated_field(ds_field["id"], new_agg="sum")
            .build_instance(f"indicator_{ds_field['id']}_sum")
            for ds_field in ds["fields"]
            if ds_field["cast"] in ["integer", "float"]
        ]
        assert len(chart_instances) > 0

        dash = (
            DashJSONBuilderSingleTab([ch_inst["name"] for ch_inst in chart_instances])
            .with_fill_defaults(fill_defaults)
            .build_instance("main_dash")
        )

        return dict(
            datasets=[ds_instance],
            charts=chart_instances,
            dashboards=[dash],
        )


@attr.s
class RequestsBuilder:
    rpc_endpoint: str = attr.ib()
    project_id: str = attr.ib()

    def workbook_create(self, wb_title: str) -> SimpleRequest:
        return SimpleRequest(
            url=self.rpc_endpoint,
            method="post",
            data_json=dict(
                kind="wb_create",
                project_id=self.project_id,
                workbook_title=wb_title,
            ),
        )

    def connection_create(
        self, wb_id: str, name: str, connection_params: dict[str, str], secret: dict[str, str]
    ) -> SimpleRequest:
        return SimpleRequest(
            url=self.rpc_endpoint,
            method="post",
            data_json=dict(
                kind="connection_create",
                workbook_id=wb_id,
                connection=dict(
                    name=name,
                    connection=connection_params,
                ),
                secret=secret,
            ),
        )

    def connection_modify(
        self,
        wb_id: str,
        name: str,
        connection_params: dict[str, str],
        secret: dict[str, str],
    ) -> SimpleRequest:
        return SimpleRequest(
            url=self.rpc_endpoint,
            method="post",
            data_json=dict(
                kind="connection_modify",
                workbook_id=wb_id,
                connection=dict(
                    name=name,
                    connection=connection_params,
                ),
                secret=secret,
            ),
        )

    def connection_delete(self, wb_id: str, name: str) -> SimpleRequest:
        return SimpleRequest(
            url=self.rpc_endpoint,
            method="post",
            data_json=dict(
                kind="connection_delete",
                workbook_id=wb_id,
                name=name,
            ),
        )

    def connection_get(self, wb_id: str, name: str) -> SimpleRequest:
        return SimpleRequest(
            url=self.rpc_endpoint,
            method="post",
            data_json=dict(
                kind="connection_get",
                workbook_id=wb_id,
                name=name,
            ),
        )

    def advise_dataset_fields(self, wb_id: str, conn_name: str, partial_dataset: dict[str, Any]) -> SimpleRequest:
        return SimpleRequest(
            url=self.rpc_endpoint,
            method="post",
            data_json=dict(
                kind="advise_dataset_fields",
                connection_ref=dict(
                    kind="wb_ref",
                    wb_id=wb_id,
                    entry_name=conn_name,
                ),
                partial_dataset=partial_dataset,
            ),
        )

    def workbook_modify(
        self,
        wb_id: str,
        datasets: Optional[list[dict[str, Any]]] = None,
        charts: Optional[list[dict[str, Any]]] = None,
        dashboards: Optional[list[dict[str, Any]]] = None,
        force_rewrite: Optional[bool] = None,
    ) -> SimpleRequest:
        return SimpleRequest(
            url=self.rpc_endpoint,
            method="post",
            data_json=dict(
                kind="wb_modify",
                workbook_id=wb_id,
                workbook=dict(
                    datasets=datasets or [],
                    charts=charts or [],
                    dashboards=dashboards or [],
                ),
                force_rewrite=force_rewrite or False,
            ),
        )

    def workbook_get(
        self,
        wb_id: str,
    ) -> SimpleRequest:
        return SimpleRequest(
            url=self.rpc_endpoint,
            method="post",
            data_json=dict(
                kind="wb_read",
                workbook_id=wb_id,
            ),
        )

    def workbook_delete(
        self,
        wb_id: str,
    ) -> SimpleRequest:
        return SimpleRequest(
            url=self.rpc_endpoint,
            method="post",
            data_json=dict(
                kind="wb_delete",
                workbook_id=wb_id,
            ),
        )
