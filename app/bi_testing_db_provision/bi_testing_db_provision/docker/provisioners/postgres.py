from __future__ import annotations

import logging
from typing import Dict, Optional

import attr

from bi_testing_db_provision.docker.provisioners.base import DefaultProvisioner, QueryExecutionException
from bi_testing_db_provision.docker.provisioners.utils import decode_output
from bi_testing_db_provision.model.commons_db import DBCreds

LOGGER = logging.getLogger(__name__)


@attr.s
class PGProvisioner(DefaultProvisioner):
    bootstrap_db_name = 'bi_testing_db_provision'
    bootstrap_db_creds = DBCreds(
        username="db_provisioner",
        password="generatedPassword_pg_505"
    )

    aliveness_check_interval = 1
    aliveness_check_query = "SELECT 1"

    def get_container_environ(self) -> Dict[str, str]:
        return dict(
            POSTGRES_DB=self.bootstrap_db_name,
            POSTGRES_USER=self.bootstrap_db_creds.username,
            POSTGRES_PASSWORD=self.bootstrap_db_creds.password,
            PGCTLTIMEOUT=300,  # type: ignore  # TODO: fix
            # To do not enter password during bootstrapping
            PGPASSWORD=self.bootstrap_db_creds.password,
        )

    def get_image_name(self) -> str:
        return {
            # image: "postgres:9.3-alpine"
            "9.3": "registry.yandex.net/statinfra/postgres@sha256:094358a1a64da927d5c26dcac9dad022cf0db840b6b627b143e5e4fd9adf982b"
        }[self._db_request.version]

    @property
    def db_client_executable(self) -> str:
        return "/usr/local/bin/psql"

    def execute_query(self, query: str, db_name: Optional[str] = None) -> None:
        effective_db_name: str = self.bootstrap_db_name if db_name is None else db_name
        args = [
            self.db_client_executable,
            "-h", "127.0.0.1",
            "-U", self.bootstrap_db_creds.username,
            "-c", query,
            effective_db_name,
        ]

        exit_code, output = self.container.exec_run(args)
        if exit_code != 0:
            raise QueryExecutionException(decode_output(output))

    def bootstrap_db(self) -> None:
        for db_name in self._db_request.db_names:
            self.execute_query(f"""CREATE DATABASE "{db_name}" """)

        # Creating users
        for creds in self._db_request.creds:
            create_query = f"CREATE USER {creds.username} WITH NOCREATEDB NOSUPERUSER NOCREATEROLE PASSWORD '{creds.password}'"
            grant_query_list = [
                f"""GRANT ALL PRIVILEGES ON DATABASE "{db_name}" TO "{creds.username}" """
                for db_name in self._db_request.db_names
            ]
            final_query = ";\n".join([create_query, *grant_query_list, ""])
            self.execute_query(final_query)
