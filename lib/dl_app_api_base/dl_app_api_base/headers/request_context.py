import http

import attr
import opentelemetry.sdk.trace

import dl_app_api_base.handlers as handlers
import dl_app_api_base.request_context as request_context
import dl_app_api_base.utils as utils
import dl_app_base
import dl_constants
import dl_utils


class UserIpNotFoundErrorResponseSchema(handlers.BadRequestResponseSchema):
    message: str = "User IP is not found"
    code: str = "ERR.API.BAD_REQUEST.USER_IP_NOT_FOUND"


@attr.define(kw_only=True, slots=False)
class HeadersRequestContextMixin(request_context.BaseRequestContext):
    _user_ip_forwarded_for_proxies_count: int = 1

    @dl_app_base.singleton_class_method_result
    def get_request_id(self) -> str:
        return (
            self._aiohttp_request.headers.get(dl_constants.DLHeadersCommon.REQUEST_ID.value)
            or dl_utils.request_id_generator()
        )

    def generate_child_request_id(self) -> str:
        request_id = self.get_request_id()
        return dl_utils.make_uuid_from_parts(
            current=dl_utils.request_id_generator(),
            parent=request_id,
        )

    @dl_app_base.singleton_class_method_result
    def get_user_ip(self) -> str:
        user_ip = utils.extract_user_ip(
            self._aiohttp_request,
            self._user_ip_forwarded_for_proxies_count,
        )

        if user_ip is None:
            raise UserIpNotFoundErrorResponseSchema().http_error(http.HTTPStatus.BAD_REQUEST)

        return user_ip

    @dl_app_base.singleton_class_method_result
    def get_trace_id(self) -> str:
        uber_trace_id = self._aiohttp_request.headers.get(dl_constants.DLHeadersCommon.UBER_TRACE_ID.value)

        if uber_trace_id:
            return uber_trace_id.split(":")[0]

        trace_id_generator = opentelemetry.sdk.trace.RandomIdGenerator()
        trace_id = trace_id_generator.generate_trace_id()
        return f"{trace_id:032x}"
