from __future__ import annotations

from http import HTTPStatus
import logging
from typing import (
    Any,
    ClassVar,
    Dict,
    Sequence,
)
import uuid

from dl_api_commons.flask.middlewares.logging_context import put_to_request_context
from dl_api_commons.flask.required_resources import RequiredResourceCommon
from dl_api_connector.api_schema.top_level import resolve_entry_loc_from_api_req_body
from dl_api_lib import (
    exc,
    utils,
)
from dl_api_lib.api_decorators import schematic_request
from dl_api_lib.app.control_api.resources import API
from dl_api_lib.app.control_api.resources.base import BIResource
from dl_api_lib.app.control_api.resources.dataset_base import DatasetResource
from dl_api_lib.const import DEFAULT_DATASET_LOCK_WAIT_TIMEOUT
from dl_api_lib.dataset.utils import (
    check_permissions_for_origin_sources,
    invalidate_sample_sources,
    log_dataset_field_stats,
)
from dl_api_lib.enums import USPermissionKind
from dl_api_lib.schemas import main as dl_api_main_schemas
import dl_api_lib.schemas.data
import dl_api_lib.schemas.dataset_base
import dl_api_lib.schemas.validation
from dl_constants.enums import (
    DataSourceCreatedVia,
    ManagedBy,
)
from dl_constants.exc import (
    CODE_OK,
    DEFAULT_ERR_CODE_API_PREFIX,
    GLOBAL_ERR_PREFIX,
)
from dl_core.base_models import (
    EntryLocation,
    PathEntryLocation,
    WorkbookEntryLocation,
)
from dl_core.components.accessor import DatasetComponentAccessor
from dl_core.components.editor import DatasetComponentEditor
from dl_core.constants import DatasetConstraints
from dl_core.us_dataset import Dataset
from dl_core.utils import generate_revision_id
import dl_query_processing.exc


LOGGER = logging.getLogger(__name__)

ns = API.namespace("Datasets", path="/datasets")

VALIDATION_OK_MESSAGE = "Validation was successful"


def _make_api_err_code(raw_code: Sequence[str]) -> str:
    return ".".join(
        (
            GLOBAL_ERR_PREFIX,
            DEFAULT_ERR_CODE_API_PREFIX,
            *raw_code,
        )
    )


@ns.route("/")
class DatasetCollection(DatasetResource):
    @classmethod
    def generate_dataset_location(cls, body: dict) -> EntryLocation:
        name = body.get("name", "Dataset {}".format(str(uuid.uuid4())))
        return resolve_entry_loc_from_api_req_body(
            name=name,
            workbook_id=body.get("workbook_id"),
            dir_path=body.get("dir_path", "datasets"),
        )

    @put_to_request_context(endpoint_code="DatasetCreate")
    @schematic_request(
        ns=ns,
        body=dl_api_main_schemas.CreateDatasetSchema(),
        responses={
            200: ("Success", dl_api_main_schemas.CreateDatasetResponseSchema()),
        },
    )
    def post(self, body: dict) -> dict:
        """Create dataset"""
        us_manager = self.get_us_manager()
        dataset = Dataset.create_from_dict(
            Dataset.DataModel(name=""),  # TODO: Remove name - it's not used, but is required
            ds_key=self.generate_dataset_location(body),
            us_manager=us_manager,
        )
        ds_editor = DatasetComponentEditor(dataset=dataset)

        if "created_via" in body:
            ds_editor.set_created_via(created_via=body["created_via"])

        result_schema = body["dataset"].get("result_schema", [])
        if len(result_schema) > DatasetConstraints.FIELD_COUNT_LIMIT_SOFT:
            raise dl_query_processing.exc.DatasetTooManyFieldsFatal()

        loader = self.create_dataset_api_loader()
        loader.populate_dataset_from_body(dataset=dataset, body=body["dataset"], us_manager=us_manager)

        us_manager.save(dataset)

        LOGGER.info("New dataset was saved with ID %s", dataset.uuid)

        return self.make_dataset_response_data(dataset=dataset, us_entry_buffer=us_manager.get_entry_buffer())


@ns.route("/<string:dataset_id>")
class DatasetItem(BIResource):
    @schematic_request(
        ns=ns,
        responses={
            200: ("Success", None),
            404: ("Not found", None),
        },
    )
    def delete(self, dataset_id: str) -> None:
        """Delete dataset"""
        us_manager = self.get_us_manager()
        ds, _ = DatasetResource.get_dataset(dataset_id=dataset_id, body={}, load_dependencies=False)
        utils.need_permission_on_entry(ds, USPermissionKind.admin)

        us_manager.delete(ds)


@ns.route("/<dataset_id>/fields")
class DatasetItemFields(BIResource):
    # TODO FIX: Move serialization logic to schema
    # TODO WARNING: Keep synced with async version
    @schematic_request(
        ns=ns,
        responses={
            200: ("Success", dl_api_lib.schemas.data.DatasetFieldsResponseSchema()),
        },
    )
    def get(self, dataset_id: str) -> dict:
        ds, _ = DatasetResource.get_dataset(dataset_id=dataset_id, body={}, load_dependencies=False)
        fields = [
            {
                "title": f.title,
                "guid": f.guid,
                "data_type": f.data_type,
                "hidden": f.hidden,
                "type": f.type,
                "calc_mode": f.calc_mode,
            }
            for f in ds.result_schema
            if f.managed_by == ManagedBy.user
        ]
        return {"fields": fields}


@ns.route("/<dataset_id>/copy")
class DatasetCopy(DatasetResource):
    @put_to_request_context(endpoint_code="DatasetCopy")
    @schematic_request(
        ns=ns,
        body=dl_api_lib.schemas.main.DatasetCopyRequestSchema(),
        responses={
            200: ("Success", dl_api_lib.schemas.main.DatasetCopyResponseSchema()),
            400: ("Failed", dl_api_lib.schemas.main.BadRequestResponseSchema()),
        },
    )
    def post(self, dataset_id: str, body: dict) -> dict:
        copy_us_key = body["new_key"]
        us_manager = self.get_us_manager()
        ds, _ = self.get_dataset(dataset_id=dataset_id, body={})
        orig_ds_loc = ds.entry_key
        copy_ds_loc: PathEntryLocation

        if isinstance(orig_ds_loc, PathEntryLocation):
            copy_ds_loc = PathEntryLocation(copy_us_key)
        else:
            raise exc.FeatureNotAvailable(message="Dataset copy in workbooks is not supported yet")

        utils.need_permission_on_entry(ds, USPermissionKind.edit)

        LOGGER.info("Going to copy dataset %s with new key %r", dataset_id, copy_us_key)
        ds_copy = us_manager.copy_entry(ds, key=copy_ds_loc)
        us_manager.save(ds_copy)
        LOGGER.info("Dataset copy was saved with ID %s", ds_copy.uuid)

        return self.make_dataset_response_data(dataset=ds_copy, us_entry_buffer=us_manager.get_entry_buffer())


@ns.route("/<dataset_id>/versions/<version>")
class DatasetVersionItem(DatasetResource):
    @put_to_request_context(endpoint_code="DatasetGet")
    @schematic_request(
        ns=ns,
        query=dl_api_lib.schemas.main.GetDatasetVersionQuerySchema(),
        responses={
            200: ("Success", dl_api_lib.schemas.main.GetDatasetVersionResponseSchema()),
            400: ("Failed", dl_api_lib.schemas.main.BadRequestResponseSchema()),
        },
    )
    def get(self, dataset_id: str, version: str, query: dict) -> dict:
        """Get dataset version"""
        us_manager = self.get_us_manager()

        if "rev_id" in query:
            ds, _ = self.get_dataset(dataset_id=dataset_id, body={}, params={"revId": query["rev_id"]})
            utils.need_permission_on_entry(ds, USPermissionKind.edit)
            # raw entry to avoid double deserialization
            ds_raw = us_manager.get_migrated_entry(dataset_id)
            # latest data revision_id for concurrent edit checks
            revision_id = ds_raw["data"]["revision_id"]
        else:
            ds, _ = self.get_dataset(dataset_id=dataset_id, body={})
            utils.need_permission_on_entry(ds, USPermissionKind.read)
            revision_id = ds.revision_id

        ds_dict = ds.as_dict()  # FIXME
        # TODO FIX: determine desired behaviour in case of workbooks
        ds_dict["key"] = ds.raw_us_key

        dl_loc = ds.entry_key
        if isinstance(dl_loc, WorkbookEntryLocation):
            ds_dict["workbook_id"] = dl_loc.workbook_id

        ds_dict["is_favorite"] = ds.is_favorite

        ds_dict.update(self.make_dataset_response_data(dataset=ds, us_entry_buffer=us_manager.get_entry_buffer()))
        ds_dict["dataset"]["revision_id"] = revision_id

        return ds_dict

    @put_to_request_context(endpoint_code="DatasetUpdate")
    @schematic_request(
        ns=ns,
        body=dl_api_lib.schemas.main.DatasetUpdateSchema(),
        responses={
            200: ("Success", dl_api_lib.schemas.dataset_base.DatasetContentSchema()),
            400: ("Failed", dl_api_lib.schemas.main.BadRequestResponseSchema()),
        },
    )
    def put(self, dataset_id: str, version: str, body: Dict[str, Any]) -> dict:
        """Update dataset version"""
        us_manager = self.get_us_manager()
        with us_manager.get_locked_entry_cm(Dataset, dataset_id, wait_timeout=DEFAULT_DATASET_LOCK_WAIT_TIMEOUT) as ds:
            utils.need_permission_on_entry(ds, USPermissionKind.edit)
            us_manager.load_dependencies(ds)

            result_schema = body["dataset"].get("result_schema", [])
            if len(result_schema) > DatasetConstraints.FIELD_COUNT_LIMIT_SOFT:
                raise dl_query_processing.exc.DatasetTooManyFieldsFatal()

            ds_accessor = DatasetComponentAccessor(dataset=ds)

            old_sources = ds_accessor.get_data_source_id_list()
            loader = self.create_dataset_api_loader()
            update_info = loader.populate_dataset_from_body(dataset=ds, body=body["dataset"], us_manager=us_manager)
            new_sources = update_info.added_own_source_ids + update_info.updated_own_source_ids
            invalidate_sample_sources(
                dataset=ds,
                source_ids=new_sources,
                us_manager=us_manager,
            )

            # checks that all dataset sources have read rights
            sources_to_check = set(new_sources) - set(old_sources)
            check_permissions_for_origin_sources(
                dataset=ds,
                source_ids=sources_to_check,
                permission_kind=USPermissionKind.read,
                us_entry_buffer=us_manager.get_entry_buffer(),
            )

            ds_editor = DatasetComponentEditor(dataset=ds)
            ds_editor.set_revision_id(revision_id=generate_revision_id())
            us_manager.save(ds)

            return self.make_dataset_response_data(dataset=ds, us_entry_buffer=us_manager.get_entry_buffer())


@ns.route("/export/<dataset_id>")
class DatasetExportItem(DatasetResource):
    REQUIRED_RESOURCES: ClassVar[frozenset[RequiredResourceCommon]] = frozenset(
        {RequiredResourceCommon.SKIP_AUTH, RequiredResourceCommon.US_HEADERS_TOKEN}
    )

    @put_to_request_context(endpoint_code="DatasetExport")
    @schematic_request(
        ns=ns,
        body=dl_api_lib.schemas.main.DatasetExportRequestSchema(),
        responses={
            200: ("Success", dl_api_lib.schemas.main.DatasetExportResponseSchema()),
            400: ("Failed", dl_api_lib.schemas.main.BadRequestResponseSchema()),
        },
    )
    def post(self, dataset_id: str, body: dict) -> dict:
        """Export dataset"""

        notifications = []
        us_manager = self.get_service_us_manager()
        ds, _ = self.get_dataset(dataset_id=dataset_id, body={})
        utils.need_permission_on_entry(ds, USPermissionKind.read)
        ds_dict = ds.as_dict()

        dl_loc = ds.entry_key
        ds_name = None
        if isinstance(dl_loc, WorkbookEntryLocation):
            ds_name = dl_loc.entry_name

        us_manager.load_dependencies(ds)
        ds_dict.update(
            self.make_dataset_response_data(
                dataset=ds, us_entry_buffer=us_manager.get_entry_buffer(), conn_id_mapping=body["id_mapping"]
            )
        )
        if ds_name:
            ds_dict["dataset"]["name"] = ds_name

        ds_warnings = ds.get_export_warnings_list()
        if ds_warnings:
            notifications.extend(ds_warnings)

        return dict(dataset=ds_dict["dataset"], notifications=notifications)


@ns.route("/import")
class DatasetImportCollection(DatasetResource):
    REQUIRED_RESOURCES: ClassVar[frozenset[RequiredResourceCommon]] = frozenset(
        {RequiredResourceCommon.SKIP_AUTH, RequiredResourceCommon.US_HEADERS_TOKEN}
    )

    @classmethod
    def generate_dataset_location(cls, body: dict) -> EntryLocation:
        name = body.get("name", "Dataset {}".format(str(uuid.uuid4())))
        return resolve_entry_loc_from_api_req_body(
            name=name,
            workbook_id=body.get("workbook_id"),
            dir_path=body.get("dir_path", "datasets"),
        )

    @classmethod
    def replace_conn_ids(cls, data: dict, conn_id_mapping: dict) -> None:
        for sources in data["dataset"]["sources"]:
            sources["connection_id"] = conn_id_mapping[sources["connection_id"]]

    @put_to_request_context(endpoint_code="DatasetImport")
    @schematic_request(
        ns=ns,
        body=dl_api_main_schemas.DatasetImportRequestSchema(),
        responses={
            200: ("Success", dl_api_main_schemas.DatasetImportResponseSchema()),
        },
    )
    def post(self, body: dict) -> dict:
        """Import dataset"""

        notifications: list[dict] = []

        name = body["data"]["dataset"].pop("name", None)
        if name:
            body["data"]["name"] = name

        data = body["data"]

        self.replace_conn_ids(data, body["id_mapping"])

        us_manager = self.get_service_us_manager()
        dataset = Dataset.create_from_dict(
            Dataset.DataModel(name=""),
            ds_key=self.generate_dataset_location(data),
            us_manager=us_manager,
        )
        ds_editor = DatasetComponentEditor(dataset=dataset)

        ds_editor.set_created_via(created_via=DataSourceCreatedVia.workbook_copy)

        result_schema = data["dataset"].get("result_schema", [])
        if len(result_schema) > DatasetConstraints.FIELD_COUNT_LIMIT_SOFT:
            raise dl_query_processing.exc.DatasetTooManyFieldsFatal()

        loader = self.create_dataset_api_loader()
        loader.populate_dataset_from_body(dataset=dataset, body=data["dataset"], us_manager=us_manager)

        us_manager.save(dataset)

        LOGGER.info("New dataset was saved with ID %s", dataset.uuid)

        ds_warnings = dataset.get_import_warnings_list()
        if ds_warnings:
            notifications.extend(ds_warnings)

        return dict(id=dataset.uuid, notifications=notifications)


@ns.route("/validators/dataset")
@ns.route("/<dataset_id>/versions/<version>/validators/schema")
class DatasetVersionValidator(DatasetResource):
    @put_to_request_context(endpoint_code="DatasetValidate")
    @schematic_request(
        ns=ns,
        body=dl_api_lib.schemas.validation.DatasetValidationSchema(),
        responses={
            200: ("Success", dl_api_lib.schemas.validation.DatasetValidationResponseSchema()),
            400: ("Failed", dl_api_lib.schemas.validation.DatasetValidationResponseSchema()),
        },
    )
    def post(
        self,
        dataset_id: str | None = None,
        version: str | None = None,
        body: dict | None = None,
    ) -> tuple[dict, HTTPStatus]:
        """Validate dataset version schema"""
        us_manager = self.get_us_manager()
        assert body is not None
        dataset, _ = self.get_dataset(dataset_id=dataset_id, body=body)
        dataset_validator_factory = self.get_service_registry().get_dataset_validator_factory()
        ds_validator = dataset_validator_factory.get_dataset_validator(ds=dataset, us_manager=us_manager)
        data = {}

        # apply updates
        try:
            ds_validator.apply_batch(action_batch=body.get("updates", ()))
        except exc.DLValidationFatal as err:
            any_errors = True
            code = _make_api_err_code(exc.DLValidationFatal.err_code)
            message = err.message
        else:
            # collect connections not found in us
            ds_validator.collect_nonexistent_connection_errors()
            ds_validator.find_and_remove_phantom_error_refs()

            # check for errors
            any_errors = bool(dataset.error_registry.items)
            code = _make_api_err_code(exc.DLValidationError.err_code) if any_errors else CODE_OK
            message = exc.DLValidationError.default_message if any_errors else VALIDATION_OK_MESSAGE
            data.update(self.make_dataset_response_data(dataset=dataset, us_entry_buffer=us_manager.get_entry_buffer()))

        data.update(
            {
                "code": code,
                "message": message,
                "dataset_errors": [],  # TODO: Remove
            }
        )
        status = HTTPStatus.OK if not any_errors else HTTPStatus.BAD_REQUEST

        log_dataset_field_stats(dataset=dataset)

        return data, status


@ns.route("/validators/field")
@ns.route("/<dataset_id>/versions/<version>/validators/field")
class DatasetVersionFieldValidator(DatasetResource):
    @put_to_request_context(endpoint_code="DatasetFieldValidate")
    @schematic_request(
        ns=ns,
        body=dl_api_lib.schemas.validation.FieldValidationSchema(),
        responses={
            200: ("Success", dl_api_lib.schemas.validation.FieldValidationResponseSchema()),
            400: ("Failed", dl_api_lib.schemas.validation.FieldValidationResponseSchema()),
        },
    )
    def post(
        self,
        *,
        dataset_id: str | None = None,
        version: str | None = None,
        body: dict | None = None,
    ) -> tuple[dict, HTTPStatus]:
        """Validate formula field of dataset version"""
        us_manager = self.get_us_manager()
        assert body is not None
        dataset, _ = self.get_dataset(dataset_id=dataset_id, body=body)
        dataset_validator_factory = self.get_service_registry().get_dataset_validator_factory()
        ds_validator = dataset_validator_factory.get_dataset_validator(ds=dataset, us_manager=us_manager)
        formula = body["field"]["formula"]
        # TODO full validation (with aggregation check for this field), not just formula
        field_errors = ds_validator.get_single_formula_errors(formula)
        code = _make_api_err_code(exc.DLValidationError.err_code) if field_errors else CODE_OK
        message = exc.DLValidationError.default_message if field_errors else VALIDATION_OK_MESSAGE
        error_data = {
            "field_errors": [
                {
                    "title": body["field"].get("title"),
                    "guid": body["field"].get("guid"),
                    "errors": field_errors,
                }
            ]
            if field_errors
            else [],
            "code": code,
            "message": message,
        }
        status = HTTPStatus.OK if not field_errors else HTTPStatus.BAD_REQUEST
        return error_data, status
