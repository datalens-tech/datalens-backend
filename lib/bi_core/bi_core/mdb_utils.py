import os
import json
import logging

import attr
import aiodns

from bi_configs.utils import split_by_comma


LOGGER = logging.getLogger(__name__)


@attr.s
class MDBDomainManagerSettings:
    managed_network_enabled: bool = attr.ib(default=True)
    mdb_domains: tuple[str, ...] = attr.ib(factory=tuple)
    mdb_cname_domains: tuple[str, ...] = attr.ib(factory=tuple)
    renaming_map: dict[str, str] = attr.ib(factory=dict)


@attr.s
class MDBDomainManager:
    settings: MDBDomainManagerSettings = attr.ib()

    @classmethod
    def from_env(cls) -> 'MDBDomainManager':  # gotta be deleted someday
        mdb_domains = split_by_comma(os.environ.get('MDB_DOMAINS', ''))
        mdb_cname_domains = split_by_comma(os.environ.get('MDB_CNAME_DOMAINS', ''))
        renaming_map = json.loads(os.environ.get('MDB_MANAGED_NETWORK_REMAP', '{}'))

        return cls(
            MDBDomainManagerSettings(
                managed_network_enabled=True,
                mdb_domains=mdb_domains,
                mdb_cname_domains=mdb_cname_domains,
                renaming_map=renaming_map,
            )
        )

    def host_in_mdb(self, host: str) -> bool:
        # WARNING: `'abcd.mdb.yandexcloud.net.'` is also a vaild hostname.
        # There should be a more correct way of doing things like that.
        return host.endswith(self.settings.mdb_domains)

    def host_is_mdb_cname(self, host: str) -> bool:
        return self.host_in_mdb(host) and host.split('.')[-4] == 'rw'

    def get_host_for_managed_network(self, host: str) -> str:
        # This hack replaces an external ("overlay", user-network) A-record
        # domain name with an internal ("underlay") AAAA-record domain name.
        # Relevant puncher access:
        # https://puncher.yandex-team.ru/?source=_DL_EXT_BACK_PROD_NETS_&destination=_CLOUD_MDB_PROD_CLIENT_NETS_
        # Relevant cloud documentation process:
        # https://st.yandex-team.ru/YCDOCS-380
        for domain in self.settings.mdb_domains:
            if host.endswith(domain):
                return host.replace(domain, self.settings.renaming_map[domain])

        raise ValueError(f'Incorrect host: {host}')

    async def normalize_mdb_host(self, original_host: str) -> str:
        working_host = original_host
        LOGGER.info("Normalizing host: %s", working_host)

        if self.host_is_mdb_cname(working_host):
            LOGGER.info("Host looks like MDB CNAME for master: %s", working_host)
            resolver = aiodns.DNSResolver()
            resp = await resolver.query(original_host, 'CNAME')
            working_host = resp.cname
            LOGGER.info("Host transformed: %s", working_host)

        if self.host_in_mdb(working_host):
            LOGGER.info("Host looks like MDB host: %s", working_host)
            working_host = self.get_host_for_managed_network(working_host)
            LOGGER.info("Host transformed: %s", working_host)

        LOGGER.info("Host normalization results: %s", working_host)
        return working_host


@attr.s
class MDBDomainManagerFactory:
    settings: MDBDomainManagerSettings = attr.ib(factory=MDBDomainManagerSettings)

    def get_manager(self) -> MDBDomainManager:
        return MDBDomainManager(self.settings)
