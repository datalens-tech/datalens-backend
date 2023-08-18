from __future__ import annotations

from bi_testing_db_provision.docker.provisioners.clickhouse import ClickHouseProvisioner
from bi_testing_db_provision.docker.provisioners.mssql import MSSQLProvisioner
from bi_testing_db_provision.docker.provisioners.oracle import OracleProvisioner
from bi_testing_db_provision.docker.provisioners.postgres import PGProvisioner
from bi_testing_db_provision.model.commons_db import DBCreds
from bi_testing_db_provision.model.request_db import DBRequest


def test_mssql(docker_client_per_func, removable_after_test_container_name):
    docker_cli = docker_client_per_func
    container_name = removable_after_test_container_name
    db_request = DBRequest(
        db_names=('db_1', 'db_2'),
        creds=(
            DBCreds(
                username='user_1',
                password='pwd',
            ),
            DBCreds(
                username='user_2',
                password='pwd',
            ),
        )
    )
    p = MSSQLProvisioner(
        docker_client=docker_cli,
        db_request=db_request,
    )
    p.provision(dict(main=container_name))


def test_oracle(docker_client_per_func, removable_after_test_container_name):
    docker_cli = docker_client_per_func
    container_name = removable_after_test_container_name
    db_request = DBRequest(
        db_names=(),
        creds=(
            DBCreds(
                username='user_1',
                password='pwd',
            ),
            DBCreds(
                username='user_2',
                password='pwd',
            ),
        )
    )
    p = OracleProvisioner(
        docker_client=docker_cli,
        db_request=db_request,
    )
    p.provision(dict(main=container_name))


def test_pg(docker_client_per_func, removable_after_test_container_name):
    docker_cli = docker_client_per_func
    container_name = removable_after_test_container_name
    db_request = DBRequest(
        version="9.3",
        db_names=('db_1', 'db_2'),
        creds=(
            DBCreds(
                username='user_1',
                password='pwd',
            ),
            DBCreds(
                username='user_2',
                password='pwd',
            ),
        )
    )
    p = PGProvisioner(
        docker_client=docker_cli,
        db_request=db_request,
    )
    p.provision(dict(main=container_name))


def test_ch(docker_client_per_func, removable_after_test_container_name):
    docker_cli = docker_client_per_func
    container_name = removable_after_test_container_name
    db_request = DBRequest(
        version="20.8",
        db_names=('db_1', 'db_2'),
        creds=(
            DBCreds(
                username='user_1',
                password='pwd',
            ),
            DBCreds(
                username='user_2',
                password='pwd',
            ),
        )
    )
    p = ClickHouseProvisioner(
        docker_client=docker_cli,
        db_request=db_request,
    )
    p.provision(dict(main=container_name))
