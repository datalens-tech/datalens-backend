import attrs

import dl_httpx.error_transformers.base as base
import dl_httpx.exceptions as exceptions


@attrs.define(frozen=True)
class StatusMapTransformer:
    status_map: dict[int, base.ExceptionFactoryProtocol]

    def transform(self, exception: exceptions.HttpStatusHttpxClientError) -> Exception | None:
        factory = self.status_map.get(exception.response.status_code)
        if factory is None:
            return None

        return factory(exception)
