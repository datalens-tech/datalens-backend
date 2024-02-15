from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import attr
from flask import request
from flask_restx import abort
from marshmallow import ValidationError as MValidationError

from dl_api_commons.flask.middlewares.logging_context import put_to_request_context
from dl_api_connector.api_schema.connection_base import ConnectionOptionsSchema
from dl_api_connector.api_schema.extras import (
    CreateMode,
    EditMode,
)
from dl_api_lib import exc
from dl_api_lib.api_decorators import schematic_request
from dl_api_lib.app.control_api.resources import API
from dl_api_lib.app.control_api.resources.base import BIResource
from dl_api_lib.enums import USPermissionKind
from dl_api_lib.schemas.connection import (
    ConnectionInfoSourceSchemaQuerySchema,
    ConnectionInfoSourceSchemaResponseSchema,
    ConnectionSourcesQuerySchema,
    ConnectionSourceTemplatesResponseSchema,
    GenericConnectionSchema,
)
from dl_api_lib.utils import need_permission_on_entry
from dl_constants.enums import (
    ConnectionType,
    DashSQLQueryType,
    UserDataType,
)
from dl_constants.exc import DLBaseException
from dl_core.data_source.type_mapping import get_data_source_class
from dl_core.data_source_merge_tools import make_spec_from_dict
from dl_core.exc import (
    DatabaseUnavailable,
    USPermissionRequired,
)
from dl_core.us_connection_base import ConnectionBase


if TYPE_CHECKING:
    from dl_core.connection_executors.sync_base import SyncConnExecutorBase


LOGGER = logging.getLogger(__name__)

ns = API.namespace("Connections", path="/connections")


def _handle_conn_test_exc(exception: Exception):  # type: ignore  # TODO: fix
    if isinstance(exception, DLBaseException):
        raise exception
    else:
        LOGGER.exception("Got unhandled exception")
        raise DatabaseUnavailable() from exception


@ns.route("/test_connection_params")
class ConnectionParamsTester(BIResource):
    @schematic_request(ns=ns)
    def post(self):  # type: ignore  # TODO: fix
        service_registry = self.get_service_registry()
        schema = GenericConnectionSchema(context=self.get_schema_ctx(schema_operations_mode=CreateMode.test))
        body_json = dict(request.json)  # type: ignore  # 2024-01-24 # TODO: Argument 1 to "dict" has incompatible type "Any | None"; expected "SupportsKeysAndGetItem[Any, Any]"  [arg-type]
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


@ns.route("/test_connection/<connection_id>")
class ConnectionTester(BIResource):
    @schematic_request(ns=ns)
    def post(self, connection_id):  # type: ignore  # TODO: fix
        usm = self.get_us_manager()
        service_registry = self.get_service_registry()
        conn = usm.get_by_id(connection_id, expected_type=ConnectionBase)
        conn_orig = usm.clone_entry_instance(conn)
        assert isinstance(conn_orig, ConnectionBase)  # for typing
        need_permission_on_entry(conn, USPermissionKind.read)

        body_json = dict(request.json)  # type: ignore  # 2024-01-24 # TODO: Argument 1 to "dict" has incompatible type "Any | None"; expected "SupportsKeysAndGetItem[Any, Any]"  [arg-type]

        has_changes = len(body_json.keys()) > 0
        if has_changes:
            need_permission_on_entry(conn, USPermissionKind.edit)
        schema_ctx = self.get_schema_ctx(schema_operations_mode=EditMode.test, editable_object=None)

        schema_cls = GenericConnectionSchema().get_edit_schema_cls(conn)  # type: ignore  # 2024-01-24 # TODO: Argument 1 to "get_edit_schema_cls" of "GenericConnectionSchema" has incompatible type "USEntry"; expected "ConnectionBase"  [arg-type]
        schema = schema_cls(context=schema_ctx)

        try:
            changes = schema.load(body_json)
            schema.update_object(conn, changes)
        except MValidationError as err:
            return err.messages, 400

        conn.validate_new_data_sync(services_registry=service_registry, changes=changes, original_version=conn_orig)  # type: ignore  # 2024-01-24 # TODO: "USEntry" has no attribute "validate_new_data_sync"  [attr-defined]

        try:
            conn.test(conn_executor_factory=service_registry.get_conn_executor_factory().get_sync_conn_executor)  # type: ignore  # 2024-01-24 # TODO: "USEntry" has no attribute "test"  [attr-defined]
        except NotImplementedError as err:
            raise exc.UnsupportedForEntityType(f"Connector {conn.conn_type.name} does not support testing") from err  # type: ignore  # 2024-01-24 # TODO: "USEntry" has no attribute "conn_type"  [attr-defined]
        except Exception as e:
            _handle_conn_test_exc(e)


@ns.route("/")
class ConnectionsList(BIResource):
    @put_to_request_context(endpoint_code="ConnectionCreate")
    @schematic_request(ns=ns)
    def post(self):  # type: ignore  # TODO: fix
        us_manager = self.get_us_manager()

        conn_availability = self.get_service_registry().get_connector_availability()
        conn_type_is_available = conn_availability.check_connector_is_available(
            ConnectionType[request.json.get("type")]  # type: ignore  # 2024-01-24 # TODO: Item "None" of "Any | None" has no attribute "get"  [union-attr]
        )
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
        responses={
            # 200: ('Success', GetConnectionResponseSchema()),
        },
    )
    def get(self, connection_id: str) -> dict:
        conn = self.get_us_manager().get_by_id(connection_id, expected_type=ConnectionBase)
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
    def put(self, connection_id):  # type: ignore  # TODO: fix
        us_manager = self.get_us_manager()

        with us_manager.get_locked_entry_cm(ConnectionBase, connection_id) as conn:  # type: ignore  # TODO: fix
            need_permission_on_entry(conn, USPermissionKind.edit)
            conn_orig = us_manager.clone_entry_instance(conn)
            assert isinstance(conn_orig, ConnectionBase)  # for typing
            schema_ctx = self.get_schema_ctx(schema_operations_mode=EditMode.edit, editable_object=None)

            schema_cls = GenericConnectionSchema(context=schema_ctx).get_edit_schema_cls(conn)
            schema = schema_cls(context=schema_ctx)

            try:
                changes = schema.load(request.json)  # type: ignore  # 2024-01-24 # TODO: Argument 1 to "load" of "Schema" has incompatible type "Any | None"; expected "Mapping[str, Any] | Iterable[Mapping[str, Any]]"  [arg-type]
                schema.update_object(conn, changes)
            except MValidationError as e:
                return e.messages, 400
            conn.validate_new_data_sync(  # type: ignore  # 2024-01-24 # TODO: <nothing> has no attribute "validate_new_data_sync"  [attr-defined]
                services_registry=self.get_service_registry(),
                changes=changes,
                original_version=conn_orig,
            )
            us_manager.save(conn)


def _dump_source_templates(tpls) -> dict:  # type: ignore  # TODO: fix
    if tpls is None:
        return None  # type: ignore  # TODO: fix
    return [dict(tpl._asdict(), parameter_hash=tpl.get_param_hash()) for tpl in tpls]  # type: ignore  # TODO: fix


@ns.route("/<connection_id>/info/metadata_sources")
class ConnectionInfoMetadataSources(BIResource):
    @schematic_request(ns=ns, responses={200: ("Success", ConnectionSourceTemplatesResponseSchema())})
    def get(self, connection_id):  # type: ignore  # TODO: fix
        connection = self.get_us_manager().get_by_id(connection_id, expected_type=ConnectionBase)

        localizer = self.get_service_registry().get_localizer()
        source_template_templates = connection.get_data_source_template_templates(localizer=localizer)  # type: ignore  # 2024-01-24 # TODO: "USEntry" has no attribute "get_data_source_template_templates"  [attr-defined]

        source_templates = []
        try:
            need_permission_on_entry(connection, USPermissionKind.read)
        except USPermissionRequired:
            pass
        else:
            source_templates = connection.get_data_source_local_templates()  # type: ignore  # 2024-01-24 # TODO: "USEntry" has no attribute "get_data_source_local_templates"  [attr-defined]

        return {
            "sources": _dump_source_templates(source_templates),
            "freeform_sources": _dump_source_templates(source_template_templates),
        }


@ns.route("/<connection_id>/info/sources")
class ConnectionInfoSources(BIResource):
    @schematic_request(
        ns=ns,
        query=ConnectionSourcesQuerySchema(),
        responses={200: ("Success", ConnectionSourceTemplatesResponseSchema())},
    )
    def get(self, connection_id, query):  # type: ignore  # TODO: fix
        connection = self.get_us_manager().get_by_id(connection_id, expected_type=ConnectionBase)

        service_registry = self.get_service_registry()
        localizer = service_registry.get_localizer()
        source_template_templates = connection.get_data_source_template_templates(localizer=localizer)  # type: ignore  # 2024-01-24 # TODO: "USEntry" has no attribute "get_data_source_template_templates"  [attr-defined]

        source_templates = []
        try:
            need_permission_on_entry(connection, USPermissionKind.read)
        except USPermissionRequired:
            pass
        else:
            source_templates = connection.get_data_source_templates(  # type: ignore  # 2024-01-24 # TODO: "USEntry" has no attribute "get_data_source_templates"  [attr-defined]
                conn_executor_factory=service_registry.get_conn_executor_factory().get_sync_conn_executor,
            )

        search_text = query.get("search_text")
        if search_text is not None:
            # TODO: for some connections, do the filtering in the database.
            search_text = search_text.lower()
            source_templates = [item for item in source_templates if search_text in item.title.lower()]
        limit = query.get("limit")
        if limit is not None:  # Note that `load_default=1000` in the schema, so it should always be defined.
            source_templates = source_templates[:limit]
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
    def post(self, connection_id: str, body: dict):  # type: ignore  # TODO: fix
        connection = self.get_us_manager().get_by_id(connection_id, expected_type=ConnectionBase)
        sr = self.get_service_registry()

        def conn_executor_factory_func() -> SyncConnExecutorBase:
            conn_executor = sr.get_conn_executor_factory().get_sync_conn_executor(conn=connection)  # type: ignore  # 2024-01-24 # TODO: Argument "conn" to "get_sync_conn_executor" of "ConnExecutorFactory" has incompatible type "USEntry"; expected "ConnectionBase"  [arg-type]
            return conn_executor

        need_permission_on_entry(connection, USPermissionKind.read)
        if not connection.is_dashsql_allowed:  # type: ignore  # 2024-01-24 # TODO: "USEntry" has no attribute "is_dashsql_allowed"  [attr-defined]
            abort(400, "Not supported for connector without dashsql allowed")

        src = body["source"]
        dsrc_spec = make_spec_from_dict(source_type=src["source_type"], data=src["parameters"])
        dsrc_cls = get_data_source_class(src["source_type"])
        dsrc = dsrc_cls(spec=dsrc_spec, connection=connection)  # type: ignore  # 2024-01-30 # TODO: Argument "connection" to "DataSource" has incompatible type "USEntry"; expected "ConnectionBase | None"  [arg-type]

        schema_info = dsrc.get_schema_info(conn_executor_factory=conn_executor_factory_func)

        return {
            "raw_schema": schema_info.schema,
        }
