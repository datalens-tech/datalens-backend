from __future__ import annotations

import logging
from typing import ClassVar, Sequence, Any

import attr
import opentracing

from bi_api_commons.logging import RequestLoggingContextController

LOGGER = logging.getLogger(__name__)


@attr.s
class OpenTracingLoggingContextController(RequestLoggingContextController):
    root_span: opentracing.Span = attr.ib()

    root_span_tags: ClassVar[Sequence[str]] = (
        'dataset_id',
        'conn_id',
        'conn_type',
        'request_id',
        'parent_request_id',
    )
    span_tag_prefix: ClassVar[str] = 'bi'

    def put_to_context(self, key: str, value: Any) -> None:
        if key in self.root_span_tags:
            self.root_span.set_tag(f"{self.span_tag_prefix}.{key}", value)
        elif key == 'endpoint_code':
            if isinstance(value, str):
                self.root_span.set_operation_name(value)
            else:
                LOGGER.warning("Got not-string value for root span operation name : %s", value)
        elif key == 'is_error':
            self.root_span.set_tag(opentracing.span.tags.ERROR, bool(value))
