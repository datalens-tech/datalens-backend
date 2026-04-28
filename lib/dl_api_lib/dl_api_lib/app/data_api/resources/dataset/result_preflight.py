from copy import deepcopy
from http import HTTPStatus
import logging

from aiohttp import web

from dl_api_lib import exc
from dl_api_lib.app.data_api.resources.base import (
    RequiredResourceDSAPI,
    requires,
)
from dl_api_lib.app.data_api.resources.dataset.base import DatasetDataBaseView
import dl_api_lib.schemas.data
from dl_api_lib.schemas.result_preflight import ResultPreflightResponseSchema
from dl_api_lib.service_registry.service_registry import ApiServiceRegistry
from dl_app_tools.profiling_base import generic_profiler_async
from dl_constants.api_constants import DLHeadersCommon
from dl_constants.exc import (
    CODE_OK,
    DEFAULT_GLOBAL_ERR_CODE_API_PREFIX,
)
from dl_core.component_errors import ComponentErrorRegistry


LOGGER = logging.getLogger(__name__)

VALIDATION_OK_MESSAGE = "Validation was successful"

_RESPONSE_SCHEMA = ResultPreflightResponseSchema()


class DatasetResultPreflightView(DatasetDataBaseView):
    endpoint_code = "DatasetVersionResultPreflight"
    profiler_prefix = "result-preflight"

    @generic_profiler_async("ds-result-preflight-full")
    @requires(RequiredResourceDSAPI.JSON_REQUEST)
    async def post(self) -> web.Response:
        if self.dataset_id is not None:
            connection_headers = {
                DLHeadersCommon.DATASET_ID.value: self.dataset_id,
            }
            self.dl_request.us_manager.set_context("connection", connection_headers)

        schema = dl_api_lib.schemas.data.ResultDataRequestV2Schema()
        req_model = schema.load(self.dl_request.json)

        try:
            await self.prepare_dataset_with_mutation_cache(req_model=req_model)
        except (exc.DLValidationFatal, exc.DatasetActionNotAllowedError) as err:
            return self._make_response(
                code=self._format_err_code(err.err_code),
                message=err.message,
                registry=ComponentErrorRegistry(),
                status=HTTPStatus.BAD_REQUEST,
            )

        services_registry = self.dl_request.services_registry
        assert isinstance(services_registry, ApiServiceRegistry)
        us_manager = self.dl_request.us_manager
        ds_validator = services_registry.get_dataset_validator_factory().get_dataset_validator(
            ds=self.dataset,
            us_manager=us_manager,
            is_data_api=True,
        )
        ds_validator.find_and_remove_phantom_error_refs()

        registry = self._prefixed_registry_copy()
        any_errors = bool(registry.items)
        code = self._format_err_code(exc.DLValidationError.err_code) if any_errors else CODE_OK
        message = exc.DLValidationError.default_message if any_errors else VALIDATION_OK_MESSAGE
        return self._make_response(
            code=code,
            message=message,
            registry=registry,
            status=HTTPStatus.BAD_REQUEST if any_errors else HTTPStatus.OK,
        )

    @staticmethod
    def _format_err_code(raw_code: list[str]) -> str:
        return ".".join((*DEFAULT_GLOBAL_ERR_CODE_API_PREFIX, *raw_code))

    @staticmethod
    def _make_response(
        *,
        code: str,
        message: str,
        registry: ComponentErrorRegistry,
        status: HTTPStatus,
    ) -> web.Response:
        body = _RESPONSE_SCHEMA.dump(
            {
                "code": code,
                "message": message,
                "dataset": {"component_errors": registry},
            }
        )
        return web.json_response(body, status=status)

    def _prefixed_registry_copy(self) -> ComponentErrorRegistry:
        registry_copy = deepcopy(self.dataset.error_registry)
        prefix_list = list(DEFAULT_GLOBAL_ERR_CODE_API_PREFIX)
        for item in registry_copy.items:
            for error in item.errors:
                if error.code[: len(prefix_list)] != prefix_list:
                    error.code = prefix_list + error.code
        return registry_copy
