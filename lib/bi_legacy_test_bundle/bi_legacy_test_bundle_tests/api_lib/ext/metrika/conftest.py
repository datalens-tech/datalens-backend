from __future__ import annotations

from contextlib import contextmanager

import pytest
import sqlalchemy_metrika_api

from bi_legacy_test_bundle_tests.api_lib.utils import (
    APPMETRICA_SAMPLE_COUNTER_ID,
    METRIKA_SAMPLE_COUNTER_ID,
)

from bi_connector_metrica.core.testing.connection import (
    make_saved_appmetrica_api_connection,
    make_saved_metrika_api_connection,
)


@contextmanager
def fake_app_context():
    yield


@pytest.fixture(scope="function")
def shrink_metrika_default_date_period(monkeypatch):
    """
    To reduce load to Metrika API and tests run time.
    """
    monkeypatch.setattr(sqlalchemy_metrika_api.base, "DEFAULT_DATE_PERIOD", 3)


@pytest.fixture(scope="function")
def metrika_api_connection_id(default_sync_usm_per_test, env_param_getter, shrink_metrika_default_date_period):
    with fake_app_context():
        conn = make_saved_metrika_api_connection(
            default_sync_usm_per_test,
            counter_id=METRIKA_SAMPLE_COUNTER_ID,
            token=env_param_getter.get_str_value("METRIKA_OAUTH"),
        )
    return conn.uuid


@pytest.fixture(scope="function")
def appmetrica_api_connection_id(default_sync_usm_per_test, env_param_getter, shrink_metrika_default_date_period):
    with fake_app_context():
        conn = make_saved_appmetrica_api_connection(
            default_sync_usm_per_test,
            counter_id=APPMETRICA_SAMPLE_COUNTER_ID,
            token=env_param_getter.get_str_value("METRIKA_OAUTH"),
        )
    return conn.uuid
