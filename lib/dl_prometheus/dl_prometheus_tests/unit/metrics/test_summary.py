import prometheus_client.samples

import dl_prometheus


def test_summary_observe() -> None:
    summary = dl_prometheus.Summary(name="payload_bytes", documentation="payload size")
    metrics_registry = dl_prometheus.MetricsRegistry(metrics=(summary,))
    summary.observe(100.0)
    summary.observe(50.0)

    assert metrics_registry.get_samples() == [
        prometheus_client.samples.Sample(name="payload_bytes_count", labels={}, value=2.0),
        prometheus_client.samples.Sample(name="payload_bytes_sum", labels={}, value=150.0),
    ]
