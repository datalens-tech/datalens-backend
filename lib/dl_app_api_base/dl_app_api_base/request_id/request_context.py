import dl_app_api_base.request_context as request_context
import dl_app_base
import dl_constants
import dl_utils


class RequestIdRequestContextMixin(request_context.BaseRequestContext):
    @dl_app_base.singleton_class_method_result
    def get_request_id(self) -> str:
        return (
            self._aiohttp_request.headers.get(dl_constants.DLHeadersCommon.REQUEST_ID.value)
            or dl_utils.request_id_generator()
        )
