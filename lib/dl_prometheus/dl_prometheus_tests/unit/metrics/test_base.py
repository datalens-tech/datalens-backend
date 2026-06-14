import prometheus_client
import prometheus_client.samples
import pytest

import dl_prometheus


def test_unregistered_metric_raises_on_use() -> None:
    counter = dl_prometheus.Counter(name="dangling_total", documentation="dangling")
    with pytest.raises(RuntimeError):
        counter.inc()


def test_rejects_labels_when_none_declared() -> None:
    counter = dl_prometheus.Counter(name="hits_total", documentation="hits")
    dl_prometheus.MetricsRegistry(metrics=(counter,))
    with pytest.raises(ValueError, match=r"has no labelnames"):
        counter.inc(labels={"method": "GET"})


def test_requires_labels_when_declared() -> None:
    counter = dl_prometheus.Counter(
        name="requests_total",
        documentation="requests",
        labelnames=("method",),
    )
    dl_prometheus.MetricsRegistry(metrics=(counter,))
    with pytest.raises(ValueError, match=r"requires labels"):
        counter.inc()


def test_rejects_label_name_mismatch() -> None:
    counter = dl_prometheus.Counter(
        name="requests_total",
        documentation="requests",
        labelnames=("method",),
    )
    dl_prometheus.MetricsRegistry(metrics=(counter,))
    with pytest.raises(ValueError, match=r"labels mismatch"):
        counter.inc(labels={"path": "/"})


def test_metric_appears_after_registry_construction() -> None:
    counter = dl_prometheus.Counter(name="auto_registered_total", documentation="x")
    metrics_registry = dl_prometheus.MetricsRegistry(metrics=(counter,))
    assert metrics_registry.get_samples() == [
        prometheus_client.samples.Sample(name="auto_registered_total", labels={}, value=0.0),
    ]


def test_can_register_with_external_collector_registry() -> None:
    counter = dl_prometheus.Counter(name="external_total", documentation="x")
    collector_registry = prometheus_client.CollectorRegistry()
    counter.register(collector_registry)
    counter.inc(2.0)

    samples = [sample for metric in collector_registry.collect() for sample in metric.samples]
    assert samples == [
        prometheus_client.samples.Sample(name="external_total", labels={}, value=2.0),
    ]


def test_register_twice_raises() -> None:
    counter = dl_prometheus.Counter(name="double_register_total", documentation="x")
    collector_registry = prometheus_client.CollectorRegistry()
    counter.register(collector_registry)
    with pytest.raises(RuntimeError):
        counter.register(prometheus_client.CollectorRegistry())
