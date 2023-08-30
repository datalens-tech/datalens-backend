from __future__ import annotations

from typing import Optional, Dict

import opentracing


def get_current_tracing_headers(tracer: Optional[opentracing.Tracer] = None) -> Dict[str, str]:
    actual_tracer = opentracing.global_tracer() if tracer is None else tracer
    tracing_headers: Dict[str, str] = {}
    active_span = actual_tracer.active_span

    if active_span is not None:
        actual_tracer.inject(active_span.context, opentracing.Format.HTTP_HEADERS, tracing_headers)

    return tracing_headers
