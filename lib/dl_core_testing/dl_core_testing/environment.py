from __future__ import annotations

import logging
import os
import time
from typing import Optional

import requests

from dl_core.logging_config import setup_jaeger_client
from dl_core_testing.configuration import UnitedStorageConfiguration
from dl_db_testing.loader import load_db_testing_lib
from dl_utils.wait import wait_for


LOGGER = logging.getLogger(__name__)


def _wait_for_pg(dsn: str, timeout: int = 300, interval: float = 1.0) -> None:
    import psycopg2

    start_time = time.monotonic()
    max_time = start_time + timeout
    while True:
        loop_start_time = time.monotonic()
        try:
            conn = psycopg2.connect(dsn)
            cur = conn.cursor()
            cur.execute("select 1./2")
            res = cur.fetchall()
        except Exception as exc:
            now = time.monotonic()
            if now > max_time:
                raise  # timed out
            next_try_time = loop_start_time + interval
            sleep_time = next_try_time - now
            if sleep_time > 0.001:
                LOGGER.debug("Waiting for pg to come up (%r)", exc)
                time.sleep(sleep_time)
        else:
            if res == [(0.5,)]:
                return
            else:
                raise Exception("Unexpected result", res)


def restart_container(container_name: str) -> None:
    import docker

    docker_cli = docker.from_env(timeout=300)
    container = docker_cli.containers.list(filters=dict(name=container_name), all=True)[0]
    container.restart()


def restart_container_by_label(label: str, compose_project: str) -> None:
    import docker

    docker_cli = docker.from_env(timeout=300)
    container = docker_cli.containers.list(
        filters=dict(
            label=[
                f"datalens.ci.service={label}",
                f"com.docker.compose.project={compose_project}",
            ]
        ),
        all=True,
    )[0]
    container.restart()


def prepare_united_storage(
    *,
    us_host: str,
    us_master_token: str,
    tenant_id: str = "common",
    us_pg_dsn: Optional[str] = None,
    force: bool = False,
) -> None:
    if not force and not os.environ.get("CLEAR_US_DATABASE", ""):
        LOGGER.debug("prepare_united_storage: CLEAR_US_DATABASE env is disabled, skipping.")
        return

    if us_pg_dsn is not None:
        LOGGER.debug("prepare_united_storage: wait for pg-us to be up...")
        _wait_for_pg(us_pg_dsn)

    headers = {
        "X-US-Master-Token": us_master_token,
        "X-DL-TenantId": tenant_id,
    }

    def _wait_for_us() -> tuple[bool, str]:
        try:
            with requests.Session() as reqr:
                resp = reqr.get(f"{us_host}/ping-db", headers=headers)
                resp.raise_for_status()
                return True, ""
        except Exception as e:
            return False, str(e)

    max_wait_time = 160.0
    wait_pause = 0.5
    LOGGER.debug(f"prepare_united_storage: waiting for up to {max_wait_time}s for US to respond with ok...")
    wait_for("US startup", condition=_wait_for_us, timeout=max_wait_time, interval=wait_pause)

    LOGGER.debug("prepare_united_storage: done.")


def prepare_united_storage_from_config(us_config: UnitedStorageConfiguration) -> None:
    prepare_united_storage(
        us_host=us_config.us_host,
        us_master_token=us_config.us_master_token,
        us_pg_dsn=us_config.us_pg_dsn,
        force=us_config.force,
    )


def common_pytest_configure(
    use_jaeger_tracer: bool = False,
    tracing_service_name: str = "tests",
) -> None:
    load_db_testing_lib()

    if use_jaeger_tracer:
        setup_jaeger_client(tracing_service_name)
