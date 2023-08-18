from __future__ import annotations

import logging
from typing import Dict, Optional

import attr

from bi_testing_db_provision.docker.provisioners.base import DefaultProvisioner, QueryExecutionException
from bi_testing_db_provision.docker.provisioners.utils import decode_output
from bi_testing_db_provision.model.commons_db import DBCreds

LOGGER = logging.getLogger(__name__)


@attr.s
class MSSQLProvisioner(DefaultProvisioner):
    bootstrap_db_name = None
    bootstrap_db_creds = DBCreds(
        username="sa",
        password="generatedPassword_505"
    )

    aliveness_check_interval = 5
    aliveness_check_query = "SELECT 1"

    def get_container_environ(self) -> Dict[str, str]:
        return dict(
            ACCEPT_EULA='Y',
            SA_PASSWORD=self.bootstrap_db_creds.password,
        )

    def get_image_name(self) -> str:
        # TODO FIX: Make depends on version
        return (
            # image: "microsoft/mssql-server-linux:2017-CU12"
            "registry.yandex.net/statinfra/mssql-server-linux"
            "@sha256:6522290393006d93b88f63a295c5137010e4e0fea548d3fce9892c07262f7a1a"
        )

    @property
    def db_client_executable(self) -> str:
        return "/opt/mssql-tools/bin/sqlcmd"

    def execute_query(self, query: str, db_name: Optional[str] = None) -> None:
        args = [
            self.db_client_executable,
            "-U", self.bootstrap_db_creds.username,
            "-P", self.bootstrap_db_creds.password,
        ]
        if db_name is not None:
            args.extend(["-d", db_name])

        args.extend(["-Q", query])

        exit_code, output = self.container.exec_run(args)
        if exit_code != 0:
            raise QueryExecutionException(decode_output(output))

    def bootstrap_db(self) -> None:
        # Creating databases
        for db_name in self._db_request.db_names:
            query = (
                "sp_configure 'contained database authentication', 1;"
                f" RECONFIGURE; IF db_id(N'{db_name}') IS NULL CREATE DATABASE {db_name} CONTAINMENT = PARTIAL"
            )
            self.execute_query(query)

        # Creating users
        for creds in self._db_request.creds:
            create_query = f"CREATE USER {creds.username} WITH PASSWORD = '{creds.password}'"
            grant_query = f"GRANT CREATE TABLE, CREATE VIEW, ALTER, INSERT, SELECT, UPDATE, DELETE TO {creds.username}"
            self.execute_query(create_query)
            self.execute_query(grant_query)
