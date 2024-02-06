from __future__ import annotations

from marshmallow import Schema
from marshmallow import fields as ma_fields

from dl_api_connector.form_config.models.base import ConnectionFormMode
from dl_api_lib.api_decorators import schematic_request
from dl_api_lib.app.control_api.resources import API
from dl_api_lib.app.control_api.resources.base import BIResource
from dl_api_lib.connection_forms.registry import (
    NoForm,
    get_connection_form_factory_cls,
)
from dl_api_lib.enums import BI_TYPE_AGGREGATIONS
from dl_api_lib.exc import UnsupportedForEntityType
from dl_api_lib.public.entity_usage_checker import PublicEnvEntityUsageChecker
from dl_api_lib.schemas.main import BadRequestResponseSchema
from dl_constants.enums import (
    ConnectionType,
    UserDataType,
)
from dl_core.exc import EntityUsageNotAllowed
from dl_core.us_dataset import Dataset


class FieldTypeInfoSchema(Schema):
    name = ma_fields.String()
    aggregations = ma_fields.List(ma_fields.String())


class GetFieldTypeCollectionResponseSchema(Schema):
    types = ma_fields.Nested(FieldTypeInfoSchema, many=True)


class DatasetsPublicityCheckerRequestSchema(Schema):
    datasets = ma_fields.List(ma_fields.String())


class DatasetsPublicityCheckerResponseSchema(Schema):
    class DatasetResponseSchema(Schema):
        dataset_id = ma_fields.String()
        allowed = ma_fields.Boolean()
        reason = ma_fields.String()

    result = ma_fields.Nested(DatasetResponseSchema, many=True)


ns = API.namespace("Info", path="/info")


@ns.route("/field_types")
class FieldTypeCollection(BIResource):
    @schematic_request(ns=ns, responses={200: ("Success", GetFieldTypeCollectionResponseSchema())})
    def get(self):  # type: ignore  # TODO: fix
        return {
            "types": [
                {"name": k.name, "aggregations": [x.name for x in v]}
                for k, v in BI_TYPE_AGGREGATIONS.items()
                if k
                not in (
                    UserDataType.uuid,
                    UserDataType.markup,
                    UserDataType.datetimetz,
                    UserDataType.datetime,
                    UserDataType.unsupported,
                )
            ]
        }


@ns.route("/datasets_publicity_checker")
class DatasetsPublicityChecker(BIResource):
    @schematic_request(
        ns=ns,
        body=DatasetsPublicityCheckerRequestSchema(),
        responses={200: ("Success", DatasetsPublicityCheckerResponseSchema())},
    )
    def post(self, body):  # type: ignore  # TODO: fix
        ds_ids = body["datasets"]
        responses = []
        us_manager = self.get_us_manager()

        public_usage_checker = PublicEnvEntityUsageChecker()

        for ds in self.get_us_manager().get_collection(
            Dataset, raise_on_broken_entry=True, include_data=True, ids=ds_ids
        ):
            try:
                us_manager.load_dependencies(ds)
                public_usage_checker.ensure_dataset_can_be_used(
                    rci=self.get_current_rci(),
                    dataset=ds,
                    us_manager=us_manager,
                )
            except EntityUsageNotAllowed as exc:
                allowed = False
                reason = exc.message
            else:
                allowed = True
                reason = None  # type: ignore  # TODO: fix

            responses.append(
                {
                    "dataset_id": ds.uuid,
                    "allowed": allowed,
                    "reason": reason,
                }
            )

        return {"result": responses}


@ns.route("/connectors")
class AvailableConnectorsCollection(BIResource):
    def get(self) -> dict:
        conn_availability = self.get_service_registry().get_connector_availability()
        localizer = self.get_service_registry().get_localizer()
        return conn_availability.as_dict(localizer=localizer)


@ns.route("/connectors/forms/<string:conn_type>/<string:form_mode>")
class ConnectorForm(BIResource):
    @schematic_request(
        ns=ns,
        responses={
            400: ("Failed", BadRequestResponseSchema()),
        },
    )
    def get(self, conn_type: str, form_mode: str) -> dict:
        try:
            ct = ConnectionType(conn_type)
        except ValueError:
            raise UnsupportedForEntityType(f"Unknown connector type: {conn_type}") from None

        try:
            mode = ConnectionFormMode(form_mode)
        except ValueError:
            raise UnsupportedForEntityType(f"Unknown form mode: {form_mode}") from None

        try:
            form_factory_cls = get_connection_form_factory_cls(ct)
        except NoForm:
            return {"form": None}

        localizer = self.get_service_registry().get_localizer()

        form_factory = form_factory_cls(mode=mode, localizer=localizer)
        form_config = form_factory.get_form_config(
            connector_settings=self.get_service_registry().get_connectors_settings(ct),
            tenant=self.get_current_rci().tenant,
        )

        return {
            "form": form_config.as_dict(),
        }


@ns.route("/internal/pseudo_workbook/<path:us_path>")
class WorkbookInfo(BIResource):
    @schematic_request(ns=ns)
    def get(self, us_path: str) -> dict:
        usm = self.get_us_manager()
        all_entries = usm.load_get_entries_at_path(us_path)

        return dict(
            connections={
                conn_dict["key"].split("/")[-1]: dict(
                    id=conn_dict["entryId"],
                    type=conn_dict["type"],
                )
                for conn_dict in all_entries
                if conn_dict["scope"] == "connection"
            },
            datasets={
                ds_dict["key"].split("/")[-1]: dict(
                    id=ds_dict["entryId"],
                )
                for ds_dict in all_entries
                if ds_dict["scope"] == "dataset"
            },
            charts={
                chart_dict["key"].split("/")[-1]: dict(
                    id=chart_dict["entryId"],
                )
                for chart_dict in all_entries
                if chart_dict["scope"] == "widget"
            },
            dashboards={
                dash["key"].split("/")[-1]: dict(
                    id=dash["entryId"],
                )
                for dash in all_entries
                if dash["scope"] == "dash"
            },
        )
