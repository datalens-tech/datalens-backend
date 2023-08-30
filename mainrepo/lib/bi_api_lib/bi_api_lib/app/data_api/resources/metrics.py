from __future__ import annotations

from bi_core.aio.metrics_view import MetricsView

from .base import BaseView


class DSDataApiMetricsView(MetricsView, BaseView):
    """View for solomon-agent, based on bi_core class"""
