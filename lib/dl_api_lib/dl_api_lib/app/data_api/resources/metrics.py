from __future__ import annotations

from dl_core.aio.metrics_view import MetricsView

from .base import BaseView


class DSDataApiMetricsView(MetricsView, BaseView):
    """View for solomon-agent, based on dl_core class"""
