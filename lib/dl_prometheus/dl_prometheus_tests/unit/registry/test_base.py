import prometheus_client
import prometheus_client.samples
import pytest

import dl_prometheus


def test_registers_and_collects() -> None:
    counter = dl_prometheus.Counter(name="events_total", documentation="event count")
    metrics_registry = dl_prometheus.MetricsRegistry(metrics=(counter,))
    counter.inc(3.0)

    assert metrics_registry.get_samples() == [
        prometheus_client.samples.Sample(name="events_total", labels={}, value=3.0),
    ]


def test_does_not_touch_global_registry() -> None:
    global_registry_size_before = len(list(prometheus_client.REGISTRY.collect()))
    counter = dl_prometheus.Counter(name="isolated_total", documentation="isolated")
    dl_prometheus.MetricsRegistry(metrics=(counter,))
    global_registry_size_after = len(list(prometheus_client.REGISTRY.collect()))

    assert global_registry_size_before == global_registry_size_after


def test_metric_cannot_be_registered_twice() -> None:
    counter = dl_prometheus.Counter(name="shared_total", documentation="shared")
    dl_prometheus.MetricsRegistry(metrics=(counter,))
    with pytest.raises(RuntimeError):
        dl_prometheus.MetricsRegistry(metrics=(counter,))


def test_multiple_metrics_in_one_registry() -> None:
    counter = dl_prometheus.Counter(name="hits_total", documentation="hits")
    gauge = dl_prometheus.Gauge(name="queue_depth", documentation="depth")
    metrics_registry = dl_prometheus.MetricsRegistry(metrics=(counter, gauge))
    counter.inc(3.0)
    gauge.set(5.0)

    assert metrics_registry.get_samples() == [
        prometheus_client.samples.Sample(name="hits_total", labels={}, value=3.0),
        prometheus_client.samples.Sample(name="queue_depth", labels={}, value=5.0),
    ]


def test_get_latest_returns_prometheus_text_exposition() -> None:
    counter = dl_prometheus.Counter(name="events_total", documentation="event count")
    metrics_registry = dl_prometheus.MetricsRegistry(metrics=(counter,))
    counter.inc(4.0)

    latest = metrics_registry.get_latest()

    assert isinstance(latest, dl_prometheus.Latest)
    assert latest.content_type.startswith("text/plain")
    body = latest.body.decode("utf-8")
    assert "events_total 4.0" in body
