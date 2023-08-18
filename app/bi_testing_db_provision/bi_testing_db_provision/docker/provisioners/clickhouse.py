from __future__ import annotations

import logging
from typing import Dict, Optional
from xml.etree import ElementTree as ET

import attr

from bi_testing_db_provision.docker.provisioners.base import DefaultProvisioner, QueryExecutionException
from bi_testing_db_provision.docker.provisioners.utils import decode_output, DockerFile, BuildContext
from bi_testing_db_provision.model.commons_db import DBCreds

LOGGER = logging.getLogger(__name__)


@attr.s
class ClickHouseProvisioner(DefaultProvisioner):
    bootstrap_db_name = None
    bootstrap_db_creds = DBCreds(
        username="default",
        password=""
    )

    aliveness_check_interval = 1
    aliveness_check_query = "SELECT 1"

    def get_container_environ(self) -> Dict[str, str]:
        return dict()

    def _get_base_image_name(self) -> str:
        return {
            # image: yandex/clickhouse-server:20.8
            "20.8": (
                "registry.yandex.net/statinfra/clickhouse-server"
                "@sha256:96d4dabcb76833af1657e705c2b9e77c74c1eda43c82d916b41dcd3dba9aec35"
            )
        }[self._db_request.version]

    def _get_users_etree(self) -> ET.Element:
        root = ET.Element('yandex')
        ET.Element('profiles')
        # Profile
        el_profiles = ET.SubElement(root, 'profiles')
        el_def_profile = ET.SubElement(el_profiles, 'default')

        ET.SubElement(el_def_profile, 'max_threads').text = '8'

        el_quotas = ET.SubElement(root, 'quotas')
        el_def_quota = ET.SubElement(el_quotas, 'default')
        el_def_quota_interval = ET.SubElement(el_def_quota, 'interval')

        ET.SubElement(el_def_quota_interval, 'duration').text = '3600'
        ET.SubElement(el_def_quota_interval, 'queries').text = '0'
        ET.SubElement(el_def_quota_interval, 'errors').text = '0'
        ET.SubElement(el_def_quota_interval, 'result_rows').text = '0'
        ET.SubElement(el_def_quota_interval, 'read_rows').text = '0'
        ET.SubElement(el_def_quota_interval, 'execution_time').text = '0'

        el_users_root = ET.SubElement(root, 'users')

        def add_user(username: str, password: str, net: str = '::/0'):  # type: ignore  # TODO: fix
            el_user = ET.SubElement(el_users_root, username)
            el_networks = ET.SubElement(el_user, 'networks')
            ET.SubElement(el_networks, 'ip').text = net
            ET.SubElement(el_user, 'password').text = password
            ET.SubElement(el_user, 'profile').text = 'default'
            ET.SubElement(el_user, 'quota').text = 'default'

        # For bootstrapping
        add_user(self.bootstrap_db_creds.username, self.bootstrap_db_creds.password, net='127.0.0.1')

        for db_creds in self._db_request.creds:
            add_user(db_creds.username, db_creds.password)

        return root

    def get_image_name(self) -> str:
        docker_file = DockerFile(self._get_base_image_name())
        with BuildContext() as context:
            context.put('users.xml', ET.tostring(self._get_users_etree()))
            docker_file.copy('users.xml', '/etc/clickhouse-server/users.xml')

            LOGGER.debug("Got docker file:\n%s", docker_file.render().decode())

            context.put('Dockerfile', docker_file.render())
            img, log_json = self._docker_client.images.build(path=context.location)

        return img.id

    @property
    def db_client_executable(self) -> str:
        return "/usr/bin/clickhouse-client"

    def execute_query(self, query: str, db_name: Optional[str] = None) -> None:
        args = [self.db_client_executable, ]
        if db_name is not None:
            args.extend(["-d", db_name])

        args.extend(["-q", query])

        exit_code, output = self.container.exec_run(args)
        if exit_code != 0:
            raise QueryExecutionException(decode_output(output))

    def bootstrap_db(self) -> None:
        for db_name in self._db_request.db_names:
            self.execute_query(f"CREATE DATABASE `{db_name}`")
