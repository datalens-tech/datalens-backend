from typing import Iterator

import prometheus_client
import pytest


@pytest.fixture(scope="session", autouse=True)
def _disable_created_metrics() -> Iterator[None]:
    # _created samples carry a creation timestamp as their value, which makes
    # list-equality assertions on get_samples() non-deterministic. Disable
    # them for the whole test session.
    prometheus_client.disable_created_metrics()
    yield
    prometheus_client.enable_created_metrics()
