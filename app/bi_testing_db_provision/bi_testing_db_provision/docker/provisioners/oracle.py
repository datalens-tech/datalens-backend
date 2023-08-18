from __future__ import annotations

import logging
import shlex
from typing import Dict, Optional

from bi_testing_db_provision.docker.provisioners.base import DefaultProvisioner, QueryExecutionException
from bi_testing_db_provision.docker.provisioners.utils import decode_output

LOGGER = logging.getLogger(__name__)


class OracleProvisioner(DefaultProvisioner):
    aliveness_check_interval = 5.
    aliveness_check_timeout = 600.

    aliveness_check_query = "SELECT 1 FROM DUAL"

    def get_container_environ(self) -> Dict[str, str]:
        return {}

    def get_image_name(self) -> str:
        # image: "registry.yandex.net/statinfra/oracle-database-enterprise:12.2.0.1-slim"
        return (
            "registry.yandex.net/statinfra/oracle-database-enterprise"
            "@sha256:25b0ec7cc3987f86b1e754fc214e7f06761c57bc11910d4be87b0d42ee12d254"
        )

    @property
    def db_client_executable(self) -> str:
        return "/u01/app/oracle/product/12.2.0/dbhome_1/bin/sqlplus"

    def bootstrap_db(self) -> None:
        for creds in self._db_request.creds:
            create_query = f"CREATE USER {creds.username} IDENTIFIED BY {creds.password}"
            grant_query_list = [
                f"GRANT CREATE SESSION TO {creds.username}",
                f"GRANT CREATE TABLE TO {creds.username}",
                f"GRANT CREATE VIEW TO {creds.username}",
                f"GRANT SELECT ON sys.V_$SESSION TO {creds.username}",
                f"ALTER USER {creds.username} QUOTA UNLIMITED ON USERS",
            ]
            query = ";\n".join([create_query, *grant_query_list, ''])
            self.execute_query(query)

    def execute_query(self, query: str, db_name: Optional[str] = None) -> None:
        assert db_name is None

        # By default sqlplus always finished with 0
        # To override this behaviour we should execute this query
        sqlplus_exit_code_hint = "WHENEVER SQLERROR EXIT SQL.SQLCODE\n"
        args = [
            "bash", "-c",
            f"echo {shlex.quote(sqlplus_exit_code_hint)} {shlex.quote(query)} |"
            f" {self.db_client_executable} -S sys/Oradoc_db1@127.0.0.1/ORCLPDB1.localdomain as sysdba"
        ]
        LOGGER.debug("Going to execute query with args: %s", args)
        exit_code, output = self.container.exec_run(args)
        decoded_output = decode_output(output)
        LOGGER.debug("Got query execution result (%d): %s", exit_code, decoded_output)
        if exit_code != 0:
            raise QueryExecutionException(output)
