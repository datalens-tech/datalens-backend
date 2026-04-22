from unittest.mock import MagicMock

import pytest

from dl_core.connection_executors.adapters.common_base import register_dialect_string

from dl_connector_metrica.core.adapters_metrica_x import MetricaAPIDefaultAdapter
from dl_connector_metrica.core.constants import CONNECTION_TYPE_METRICA_API
from dl_connector_metrica.core.target_dto import MetricaAPIConnTargetDTO


register_dialect_string(CONNECTION_TYPE_METRICA_API, "metrika_api")


def make_adapter(token: str) -> MetricaAPIDefaultAdapter:
    dto = MetricaAPIConnTargetDTO(conn_id=None, token=token, accuracy=0.0)
    return MetricaAPIDefaultAdapter(
        default_chunk_size=1,
        target_dto=dto,
        req_ctx_info=MagicMock(),
    )


@pytest.mark.parametrize(
    ("token", "expected_dsn"),
    [
        pytest.param(
            "tok en",
            "metrika_api://:tok%20en@/123456?accuracy=0.0",
            id="space-encoded-as-percent20",
        ),
        pytest.param(
            "mytoken123",
            "metrika_api://:mytoken123@/123456?accuracy=0.0",
            id="plain-token-unchanged",
        ),
    ],
)
def test_build_dsn(token: str, expected_dsn: str) -> None:
    assert make_adapter(token)._build_dsn(db_name="123456") == expected_dsn
