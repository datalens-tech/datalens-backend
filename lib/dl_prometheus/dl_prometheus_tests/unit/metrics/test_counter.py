import prometheus_client.samples

import dl_prometheus


def test_counter_without_labels() -> None:
    counter = dl_prometheus.Counter(name="hits_total", documentation="hits")
    metrics_registry = dl_prometheus.MetricsRegistry(metrics=(counter,))
    counter.inc()
    counter.inc(2.5)

    assert metrics_registry.get_samples() == [
        prometheus_client.samples.Sample(name="hits_total", labels={}, value=3.5),
    ]


def test_counter_with_labels() -> None:
    counter = dl_prometheus.Counter(
        name="requests_total",
        documentation="requests",
        labelnames=("method",),
    )
    metrics_registry = dl_prometheus.MetricsRegistry(metrics=(counter,))
    counter.inc(labels={"method": "GET"})
    counter.inc(2.0, labels={"method": "POST"})

    assert metrics_registry.get_samples() == [
        prometheus_client.samples.Sample(name="requests_total", labels={"method": "GET"}, value=1.0),
        prometheus_client.samples.Sample(name="requests_total", labels={"method": "POST"}, value=2.0),
    ]
