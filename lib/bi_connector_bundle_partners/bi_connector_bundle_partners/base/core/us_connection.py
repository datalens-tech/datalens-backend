from __future__ import annotations

import abc
from base64 import b64decode
import json
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
)

import attr
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import (
    hashes,
    serialization,
)
from cryptography.hazmat.primitives.asymmetric import padding

from dl_connector_clickhouse.core.clickhouse_base.conn_options import CHConnectOptions
from dl_connector_clickhouse.core.clickhouse_base.us_connection import ConnectionClickhouseBase
from dl_core.connection_executors.sync_base import SyncConnExecutorBase
from dl_core.us_connection_base import (
    ConnectionBase,
    ConnectionHardcodedDataMixin,
    HiddenDatabaseNameMixin,
)
from dl_core.utils import parse_comma_separated_hosts

from bi_connector_bundle_partners.base.core.settings import PartnerConnectorSettingsBase

if TYPE_CHECKING:
    from dl_connector_clickhouse.core.clickhouse_base.dto import ClickHouseConnDTO
    from dl_core.us_manager.us_manager_sync import SyncUSManager

LOGGER = logging.getLogger(__name__)


class PartnersCHConnectionBase(
    ConnectionHardcodedDataMixin[PartnerConnectorSettingsBase],
    HiddenDatabaseNameMixin,
    ConnectionClickhouseBase,
    metaclass=abc.ABCMeta,
):
    is_always_internal_source: ClassVar[bool] = True
    allow_cache: ClassVar[bool] = True
    settings_type = PartnerConnectorSettingsBase

    def get_conn_dto(self) -> ClickHouseConnDTO:
        cs = self._connector_settings
        assert self.db_name
        conn_dto = attr.evolve(
            super().get_conn_dto(),
            protocol="https" if cs.SECURE else "http",
            host=cs.HOST,
            multihosts=parse_comma_separated_hosts(cs.HOST),  # type: ignore  # TODO: fix
            port=cs.PORT,
            username=cs.USERNAME,
            password=cs.PASSWORD,
            db_name=self.db_name,
        )
        return conn_dto

    @property
    def allow_public_usage(self) -> bool:
        return self._connector_settings.ALLOW_PUBLIC_USAGE

    def get_conn_options(self) -> CHConnectOptions:
        return (
            super()
            .get_conn_options()
            .clone(
                use_managed_network=self._connector_settings.USE_MANAGED_NETWORK,
            )
        )

    def test(self, conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase]) -> None:
        """
        Don't execute `select 1` on our service databases - it's useless because user can't
        manage it anyway.
        """
        pass

    @classmethod
    def decrypt_access_token(cls, access_token: str, usm: SyncUSManager) -> dict[str, Any]:
        key_parts = access_token.split(":")
        if len(key_parts) != 4:
            LOGGER.info("Can't split")
            raise ValueError("Unable to parse access_token")
        dl_key_version = key_parts[0]
        partner_key_version = key_parts[1]
        ciphertext = b64decode(key_parts[2])
        signature = b64decode(key_parts[3])

        connector_settings = cls._get_connector_settings(usm)

        try:
            private_key_datalens_pem = connector_settings.PARTNER_KEYS.dl_private.get(dl_key_version)
            public_key_partner_pem = connector_settings.PARTNER_KEYS.partner_public.get(partner_key_version)
        except KeyError as ex:
            LOGGER.info("Unable to find key for version")
            raise ValueError("Unable to parse access_token") from ex
        assert private_key_datalens_pem
        assert public_key_partner_pem
        private_key_datalens = serialization.load_pem_private_key(private_key_datalens_pem.encode(), password=None)
        public_key_partner = serialization.load_pem_public_key(public_key_partner_pem.encode())

        try:
            public_key_partner.verify(signature, ciphertext, padding.PKCS1v15(), hashes.SHA1())
        except InvalidSignature as ex:
            LOGGER.info("Unable to verify encrypted message signature.")
            raise ValueError("Unable to parse access_token") from ex

        data = private_key_datalens.decrypt(
            ciphertext,
            padding.PKCS1v15(),
        ).decode(encoding="utf-8")
        return json.loads(data)
