from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    NoReturn,
)

from flask import request
from flask_restx import abort
from marshmallow import ValidationError as MValidationError

from dl_api_commons.flask.middlewares.logging_context import put_to_request_context
from dl_api_commons.flask.required_resources import RequiredResourceCommon
from dl_api_connector.api_schema.connection_base import ConnectionOptionsSchema
from dl_api_lib import exc
from dl_api_lib.api_decorators import schematic_request
from dl_api_lib.app.control_api.resources import API
from dl_api_lib.app.control_api.resources.base import (
    BIResource,
    wrap_export_import_exception,
)
from dl_api_lib.enums import USPermissionKind
from dl_api_lib.schemas.connection import (
    BadRequestResponseSchema,
    ConnectionDBNamesResponseSchema,
    ConnectionExportResponseSchema,
    ConnectionImportRequestSchema,
    ConnectionInfoSourceListingOptionsResponseSchema,
    ConnectionInfoSourceSchemaQuerySchema,
    ConnectionInfoSourceSchemaResponseSchema,
    ConnectionItemQuerySchema,
    ConnectionSourcesQuerySchema,
    ConnectionSourceTemplatesResponseSchema,
    GenericConnectionSchema,
)
from dl_api_lib.schemas.main import ImportResponseSchema
from dl_api_lib.utils import (
    check_permission_on_entry,
    need_permission_on_entry,
)
from dl_constants.enums import (
    ConnectionType,
    CreateMode,
    EditMode,
    ExportMode,
    ImportMode,
)
from dl_constants.exc import DLBaseException
from dl_core.connectors.base.export_import import is_export_import_allowed
from dl_core.data_source.type_mapping import get_data_source_class
from dl_core.data_source_merge_tools import make_spec_from_dict
from dl_core.exc import (
    DatabaseUnavailable,
    InvalidRequestError,
)
from dl_core.us_connection_base import (
    ConnectionBase,
    DataSourceTemplate,
    ListingOptions,
)


if TYPE_CHECKING:
    from dl_core.connection_executors.sync_base import SyncConnExecutorBase


LOGGER = logging.getLogger(__name__)

ns = API.namespace("Connections", path="/connections")


def _handle_conn_test_exc(exception: Exception) -> NoReturn:
    if isinstance(exception, DLBaseException):
        raise exception
    else:
        LOGGER.exception("Got unhandled exception")
        raise DatabaseUnavailable() from exception


@ns.route("/test_connection_params")
class ConnectionParamsTester(BIResource):
    @schematic_request(ns=ns)
    def post(self) -> None | tuple[list | dict, int]:
        service_registry = self.get_service_registry()
        schema = GenericConnectionSchema(context=self.get_schema_ctx(schema_operations_mode=CreateMode.test))
        body_json = request.get_json()
        assert isinstance(body_json, dict)
        body_json["name"] = "mocked_name"  # FIXME: BI-4639
        try:
            conn: ConnectionBase = schema.load(body_json)
            conn_type = body_json.get("type")
        except MValidationError as err:
            return err.messages, 400

        conn.validate_new_data_sync(
            services_registry=service_registry,
        )

        try:
            conn.test(conn_executor_factory=service_registry.get_conn_executor_factory().get_sync_conn_executor)
        except NotImplementedError as err:
            raise exc.UnsupportedForEntityType(f"Connector {conn_type} does not support testing") from err
        except Exception as err:
            _handle_conn_test_exc(err)
        return None


@ns.route("/test_connection/<connection_id>")
class ConnectionTester(BIResource):
    @schematic_request(ns=ns)
    def post(self, connection_id: str) -> None | tuple[list | dict, int]:
        usm = self.get_us_manager()
        service_registry = self.get_service_registry()
        conn = usm.get_by_id(connection_id, expected_type=ConnectionBase)
        conn_orig = usm.clone_entry_instance(conn)
        assert isinstance(conn_orig, ConnectionBase)  # for typing
        need_permission_on_entry(conn, USPermissionKind.read)

        body_json = request.get_json()
        assert isinstance(body_json, dict)

        has_changes = len(body_json.keys()) > 0
        if has_changes:
            need_permission_on_entry(conn, USPermissionKind.edit)
        schema_ctx = self.get_schema_ctx(schema_operations_mode=EditMode.test, editable_object=None)

        schema_cls = GenericConnectionSchema().get_edit_schema_cls(conn)
        schema = schema_cls(context=schema_ctx)

        try:
            changes = schema.load(body_json)
            schema.update_object(conn, changes)
        except MValidationError as err:
            return err.messages, 400

        conn.validate_new_data_sync(services_registry=service_registry, changes=changes, original_version=conn_orig)

        try:
            conn.test(conn_executor_factory=service_registry.get_conn_executor_factory().get_sync_conn_executor)
        except NotImplementedError as err:
            raise exc.UnsupportedForEntityType(f"Connector {conn.conn_type.name} does not support testing") from err
        except Exception as e:
            _handle_conn_test_exc(e)
        return None


@ns.route("/import")
class ConnectionsImportList(BIResource):
    REQUIRED_RESOURCES: ClassVar[frozenset[RequiredResourceCommon]] = frozenset(
        {RequiredResourceCommon.SKIP_AUTH, RequiredResourceCommon.US_HEADERS_TOKEN}
    )

    @classmethod
    def get_from_request(cls, body: Any, field_name: str) -> Any:
        if not body or type(body) != dict:
            raise InvalidRequestError("Unexpected request schema")

        field = body.get(field_name)
        if not field:
            raise InvalidRequestError(f"Missing data for required field: {field_name}")
        return field

    @put_to_request_context(endpoint_code="ConnectionImport")
    @schematic_request(
        ns=ns,
        body=ConnectionImportRequestSchema(),
        responses={
            200: ("Success", ImportResponseSchema()),
        },
    )
    @wrap_export_import_exception
    def post(self, body: dict) -> dict | tuple[list | dict, int]:
        us_manager = self.get_service_us_manager()
        tenant = self.get_current_rci().tenant
        assert tenant is not None
        us_manager.set_tenant_override(tenant)

        conn_data = body["data"]["connection"]

        conn_type_str = self.get_from_request(conn_data, "db_type")
        if conn_type_str not in ConnectionType:
            raise exc.BadConnectionType(f"Not a valid connection type for this environment: {conn_type_str}")
        conn_type = ConnectionType(conn_type_str)

        conn_availability = self.get_service_registry().get_connector_availability()
        conn_type_is_available = conn_availability.check_connector_is_available(conn_type)
        if not conn_type_is_available or not is_export_import_allowed(conn_type):
            raise exc.UnsupportedForEntityType(
                f"Connector {conn_type_str} is not available for export/import in current environment"
            )

        conn_data["workbook_id"] = body["data"].get("workbook_id")
        conn_data["type"] = conn_type_str

        schema = GenericConnectionSchema(
            context=self.get_schema_ctx(schema_operations_mode=ImportMode.create_from_import)
        )
        try:
            conn: ConnectionBase = schema.load(conn_data)
        except MValidationError as e:
            return e.messages, 400

        conn.validate_new_data_sync(services_registry=self.get_service_registry())

        notifications = []
        localizer = self.get_service_registry().get_localizer()
        conn_warnings = conn.get_import_warnings_list(localizer=localizer)
        if conn_warnings:
            notifications.extend(conn_warnings)

        us_manager.save(conn)

        return dict(id=conn.uuid, notifications=notifications)


@ns.route("/")
class ConnectionsList(BIResource):
    @put_to_request_context(endpoint_code="ConnectionCreate")
    @schematic_request(ns=ns)
    def post(self) -> dict | tuple[list | dict, int]:
        us_manager = self.get_us_manager()

        conn_availability = self.get_service_registry().get_connector_availability()
        conn_type = request.json and request.json.get("type")
        if not conn_type or conn_type not in ConnectionType:
            raise exc.BadConnectionType(f"Invalid connection type value: {conn_type}")
        conn_type_is_available = conn_availability.check_connector_is_available(ConnectionType[conn_type])

        if not conn_type_is_available:
            # TODO: remove `abort` after migration to schematic_request decorator with common error handling
            abort(400, "This connection type is not editable")
            raise exc.UnsupportedForEntityType("This connection type is not editable")

        schema = GenericConnectionSchema(context=self.get_schema_ctx(schema_operations_mode=CreateMode.create))
        try:
            conn: ConnectionBase = schema.load(request.json)
        except MValidationError as e:
            return e.messages, 400

        conn.validate_new_data_sync(services_registry=self.get_service_registry())

        us_manager.save(conn)

        return {"id": conn.uuid}


@ns.route("/<connection_id>")
class ConnectionItem(BIResource):
    @put_to_request_context(endpoint_code="ConnectionGet")
    @schematic_request(
        ns=ns,
        query=ConnectionItemQuerySchema(),
        responses={
            # 200: ('Success', GetConnectionResponseSchema()),
        },
    )
    def get(self, connection_id: str, query: dict) -> dict:
        us_manager = self.get_us_manager()

        if "rev_id" in query:
            conn = us_manager.get_by_id(
                connection_id,
                expected_type=ConnectionBase,
                params={"revId": query["rev_id"]},
            )
            need_permission_on_entry(conn, USPermissionKind.edit)
        else:
            conn = us_manager.get_by_id(connection_id, expected_type=ConnectionBase)
            need_permission_on_entry(conn, USPermissionKind.read)

        assert isinstance(conn, ConnectionBase)

        result = GenericConnectionSchema(context=self.get_schema_ctx(EditMode.edit)).dump(conn)
        result.update(options=ConnectionOptionsSchema().dump(conn.get_options()))
        return result

    @put_to_request_context(endpoint_code="ConnectionDelete")
    @schematic_request(ns=ns)
    def delete(self, connection_id: str) -> None:
        us_manager = self.get_us_manager()
        conn = us_manager.get_by_id(connection_id, expected_type=ConnectionBase)
        need_permission_on_entry(conn, USPermissionKind.admin)

        us_manager.delete(conn)

    @put_to_request_context(endpoint_code="ConnectionUpdate")
    @schematic_request(ns=ns)
    def put(self, connection_id: str) -> None | tuple[list | dict, int]:
        us_manager = self.get_us_manager()

        with us_manager.get_locked_entry_cm(ConnectionBase, connection_id) as conn:  # type: ignore  # TODO: fix
            need_permission_on_entry(conn, USPermissionKind.edit)
            conn_orig = us_manager.clone_entry_instance(conn)
            assert isinstance(conn_orig, ConnectionBase)
            schema_ctx = self.get_schema_ctx(schema_operations_mode=EditMode.edit, editable_object=None)

            schema_cls = GenericConnectionSchema(context=schema_ctx).get_edit_schema_cls(conn)
            schema = schema_cls(context=schema_ctx)

            try:
                body_json = request.get_json()
                assert isinstance(body_json, dict)
                changes = schema.load(body_json)
                schema.update_object(conn, changes)
            except MValidationError as e:
                return e.messages, 400
            assert isinstance(conn, ConnectionBase)
            conn.validate_new_data_sync(
                services_registry=self.get_service_registry(),
                changes=changes,
                original_version=conn_orig,
            )
            us_manager.save(conn)
            return None


@ns.route("/export/<connection_id>")
class ConnectionExportItem(BIResource):
    REQUIRED_RESOURCES: ClassVar[frozenset[RequiredResourceCommon]] = frozenset(
        {RequiredResourceCommon.SKIP_AUTH, RequiredResourceCommon.US_HEADERS_TOKEN}
    )

    @put_to_request_context(endpoint_code="ConnectionExport")
    @schematic_request(
        ns=ns,
        responses={
            200: ("Success", ConnectionExportResponseSchema()),
        },
    )
    @wrap_export_import_exception
    def get(self, connection_id: str) -> dict:
        us_manager = self.get_service_us_manager()
        tenant = self.get_current_rci().tenant
        assert tenant is not None
        us_manager.set_tenant_override(tenant)

        conn_us_resp = us_manager.get_by_id_raw(connection_id, expected_type=ConnectionBase)
        conn_type_str = conn_us_resp.get("type")
        if conn_type_str not in ConnectionType:
            raise exc.BadConnectionType(f"Not a valid connection type for this environment: {conn_type_str}")
        conn_type = ConnectionType(conn_type_str)

        if not is_export_import_allowed(conn_type):
            raise exc.UnsupportedForEntityType(
                f"Connector {conn_type_str} is not available for export/import in current environment"
            )

        conn: ConnectionBase = us_manager.deserialize_us_resp(conn_us_resp, expected_type=ConnectionBase)

        result = GenericConnectionSchema(context=self.get_schema_ctx(ExportMode.export)).dump(conn)
        result["db_type"] = conn.conn_type.value

        notifications: list[dict] = []
        localizer = self.get_service_registry().get_localizer()
        conn_warnings = conn.get_export_warnings_list(localizer=localizer)
        if conn_warnings:
            notifications.extend(conn_warnings)

        return dict(connection=result, notifications=notifications)


def _dump_source_templates(tpls: list[DataSourceTemplate] | None) -> list[dict[str, Any]] | None:
    if tpls is None:
        return None
    return [dict(tpl._asdict(), parameter_hash=tpl.get_param_hash()) for tpl in tpls]


@ns.route("/<connection_id>/info/metadata_sources")
class ConnectionInfoMetadataSources(BIResource):
    @schematic_request(ns=ns, responses={200: ("Success", ConnectionSourceTemplatesResponseSchema())})
    def get(self, connection_id: str) -> dict[str, list[dict[str, Any]] | None]:
        connection: ConnectionBase = self.get_us_manager().get_by_id(connection_id, expected_type=ConnectionBase)

        localizer = self.get_service_registry().get_localizer()
        source_template_templates = connection.get_data_source_template_templates(localizer=localizer)

        source_templates: list[DataSourceTemplate] | None = []
        if not check_permission_on_entry(connection, USPermissionKind.read):
            source_templates = []
        else:
            source_templates = connection.get_data_source_local_templates()

        return {
            "sources": _dump_source_templates(source_templates),
            "freeform_sources": _dump_source_templates(source_template_templates),
        }


@ns.route("/<connection_id>/db_names")
class ConnectionDBNames(BIResource):
    @schematic_request(
        ns=ns,
        responses={200: ("Success", ConnectionDBNamesResponseSchema()), 400: ("Failed", BadRequestResponseSchema())},
    )
    def get(self, connection_id: str) -> dict[str, list[str]]:
        connection = self.get_us_manager().get_by_id(connection_id, expected_type=ConnectionBase)

        service_registry = self.get_service_registry()
        if not connection.supports_db_name_listing:
            raise exc.UnsupportedForEntityType("DB names listing is not supported for current connection type")
        return {
            "db_names": connection.get_db_names(
                conn_executor_factory=service_registry.get_conn_executor_factory().get_sync_conn_executor
            )
        }


@ns.route("/<connection_id>/info/source_listing_options")
class ConnectionInfoSourceListingOptions(BIResource):
    def _prepare_response(self, listing_options: ListingOptions) -> dict[str, ListingOptions]:
        return {
            "source_listing": listing_options,
        }

    @schematic_request(
        ns=ns,
        responses={
            200: ("Success", ConnectionInfoSourceListingOptionsResponseSchema()),
            400: ("Failed", BadRequestResponseSchema()),
        },
    )
    def get(self, connection_id: str) -> dict:
        connection = self.get_us_manager().get_by_id(connection_id, expected_type=ConnectionBase)

        if not check_permission_on_entry(connection, USPermissionKind.read):
            # It does not matter what options we provide if the user does not have sufficient permissions,
            # because the listing itself will not attempt to list actual DB sources, see `/info/sources`
            listing_options = ListingOptions(
                supports_source_search=False,
                supports_source_pagination=False,
                supports_db_name_listing=False,
                db_name_required_for_search=False,
                db_name_label=None,
            )
            return self._prepare_response(listing_options)

        service_registry = self.get_service_registry()
        localizer = service_registry.get_localizer()
        listing_options = connection.get_listing_options(localizer)
        return self._prepare_response(listing_options)


@ns.route("/<connection_id>/info/sources")
class ConnectionInfoSources(BIResource):
    @schematic_request(
        ns=ns,
        query=ConnectionSourcesQuerySchema(),
        responses={
            200: ("Success", ConnectionSourceTemplatesResponseSchema()),
            400: ("Failed", BadRequestResponseSchema()),
        },
    )
    def get(self, connection_id: str, query: dict) -> dict:
        connection = self.get_us_manager().get_by_id(connection_id, expected_type=ConnectionBase)

        service_registry = self.get_service_registry()
        localizer = service_registry.get_localizer()

        # Extract query parameters
        search_text = query.get("search_text")
        limit = query.get("limit")
        offset = query.get("offset")
        db_name = query.get("db_name")

        # Get template sources (always available, no DB query needed)
        source_template_templates = connection.get_data_source_template_templates(localizer=localizer)

        # Get actual data source templates (requires DB access)
        source_templates: list[DataSourceTemplate]
        if not check_permission_on_entry(connection, USPermissionKind.read):
            source_templates = []
        else:
            source_templates = connection.get_data_source_templates_paginated(
                conn_executor_factory=service_registry.get_conn_executor_factory().get_sync_conn_executor,
                search_text=search_text,
                limit=limit,
                offset=offset,
                db_name=db_name,
            )

        return {
            "sources": _dump_source_templates(source_templates),
            "freeform_sources": _dump_source_templates(source_template_templates),
        }


@ns.route("/<connection_id>/info/source/schema")
class ConnectionInfoSourceSchema(BIResource):
    @schematic_request(
        ns=ns,
        body=ConnectionInfoSourceSchemaQuerySchema(),
        responses={200: ("Success", ConnectionInfoSourceSchemaResponseSchema())},
    )
    def post(self, connection_id: str, body: dict) -> dict:
        connection = self.get_us_manager().get_by_id(connection_id, expected_type=ConnectionBase)
        sr = self.get_service_registry()

        def conn_executor_factory_func() -> SyncConnExecutorBase:
            conn_executor = sr.get_conn_executor_factory().get_sync_conn_executor(conn=connection)
            return conn_executor

        need_permission_on_entry(connection, USPermissionKind.read)
        if not connection.is_dashsql_allowed:
            abort(400, "Not supported for connector without dashsql allowed")

        src = body["source"]
        dsrc_spec = make_spec_from_dict(source_type=src["source_type"], data=src["parameters"])
        dsrc_cls = get_data_source_class(src["source_type"])
        dsrc = dsrc_cls(
            spec=dsrc_spec,
            connection=connection,
            dataset_parameter_values={},
            dataset_template_enabled=False,
        )

        schema_info = dsrc.get_schema_info(conn_executor_factory=conn_executor_factory_func)

        return {
            "raw_schema": schema_info.schema,
        }
