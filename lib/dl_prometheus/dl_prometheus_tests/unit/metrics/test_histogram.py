import prometheus_client.samples

import dl_prometheus


def test_histogram_default_buckets() -> None:
    histogram = dl_prometheus.Histogram(name="latency_seconds", documentation="latency")
    metrics_registry = dl_prometheus.MetricsRegistry(metrics=(histogram,))
    histogram.observe(0.05)

    assert metrics_registry.get_samples() == [
        prometheus_client.samples.Sample(name="latency_seconds_bucket", labels={"le": "0.005"}, value=0.0),
        prometheus_client.samples.Sample(name="latency_seconds_bucket", labels={"le": "0.01"}, value=0.0),
        prometheus_client.samples.Sample(name="latency_seconds_bucket", labels={"le": "0.025"}, value=0.0),
        prometheus_client.samples.Sample(name="latency_seconds_bucket", labels={"le": "0.05"}, value=1.0),
        prometheus_client.samples.Sample(name="latency_seconds_bucket", labels={"le": "0.075"}, value=1.0),
        prometheus_client.samples.Sample(name="latency_seconds_bucket", labels={"le": "0.1"}, value=1.0),
        prometheus_client.samples.Sample(name="latency_seconds_bucket", labels={"le": "0.25"}, value=1.0),
        prometheus_client.samples.Sample(name="latency_seconds_bucket", labels={"le": "0.5"}, value=1.0),
        prometheus_client.samples.Sample(name="latency_seconds_bucket", labels={"le": "0.75"}, value=1.0),
        prometheus_client.samples.Sample(name="latency_seconds_bucket", labels={"le": "1.0"}, value=1.0),
        prometheus_client.samples.Sample(name="latency_seconds_bucket", labels={"le": "2.5"}, value=1.0),
        prometheus_client.samples.Sample(name="latency_seconds_bucket", labels={"le": "5.0"}, value=1.0),
        prometheus_client.samples.Sample(name="latency_seconds_bucket", labels={"le": "7.5"}, value=1.0),
        prometheus_client.samples.Sample(name="latency_seconds_bucket", labels={"le": "10.0"}, value=1.0),
        prometheus_client.samples.Sample(name="latency_seconds_bucket", labels={"le": "+Inf"}, value=1.0),
        prometheus_client.samples.Sample(name="latency_seconds_count", labels={}, value=1.0),
        prometheus_client.samples.Sample(name="latency_seconds_sum", labels={}, value=0.05),
    ]


def test_histogram_observe_with_buckets() -> None:
    histogram = dl_prometheus.Histogram(
        name="latency_seconds",
        documentation="latency",
        buckets=(0.1, 0.5, 1.0),
    )
    metrics_registry = dl_prometheus.MetricsRegistry(metrics=(histogram,))
    histogram.observe(0.5)
    histogram.observe(1.0)

    assert metrics_registry.get_samples() == [
        prometheus_client.samples.Sample(name="latency_seconds_bucket", labels={"le": "0.1"}, value=0.0),
        prometheus_client.samples.Sample(name="latency_seconds_bucket", labels={"le": "0.5"}, value=1.0),
        prometheus_client.samples.Sample(name="latency_seconds_bucket", labels={"le": "1.0"}, value=2.0),
        prometheus_client.samples.Sample(name="latency_seconds_bucket", labels={"le": "+Inf"}, value=2.0),
        prometheus_client.samples.Sample(name="latency_seconds_count", labels={}, value=2.0),
        prometheus_client.samples.Sample(name="latency_seconds_sum", labels={}, value=1.5),
    ]
