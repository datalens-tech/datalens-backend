import prometheus_client.samples

import dl_prometheus


def test_gauge_set_inc_dec() -> None:
    gauge = dl_prometheus.Gauge(name="queue_depth", documentation="queue depth")
    metrics_registry = dl_prometheus.MetricsRegistry(metrics=(gauge,))
    gauge.set(10.0)
    gauge.inc(2.0)
    gauge.dec(5.0)

    assert metrics_registry.get_samples() == [
        prometheus_client.samples.Sample(name="queue_depth", labels={}, value=7.0),
    ]
