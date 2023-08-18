import asyncio
import argparse
import logging
import sys

from aiopg.sa import create_engine
from sqlalchemy.sql import or_
from typing import Optional, List

from bi_alerts.settings import from_granular_settings
from bi_alerts.db_manager import DbManager
from bi_alerts.models.db import BIDatasync
from bi_core.us_manager.crypto.main import CryptoController, EncryptedData
from bi_configs.crypto_keys import get_crypto_keys_config_from_env


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
LOGGER = logging.getLogger(__name__)


class BIDatasyncCryptoManager(DbManager):
    _model = BIDatasync
    _whitelist = {
        'oauth_encrypted',
        'crypto_key_id',
    }

    async def get_objects(
        self,
        crypto_key_id: str,
    ) -> Optional[List[BIDatasync]]:
        result = await self.db_conn.execute(
            self._model.select_().where(
                or_(
                    self._model.crypto_key_id != crypto_key_id,
                    self._model.crypto_key_id == None,
                ),
            )
        )
        return await result.fetchall()


def get_crypto_data(
    crypto: CryptoController,
    datasync: BIDatasync,
) -> Optional[EncryptedData]:
    if datasync.crypto_key_id is None:
        result = crypto.encrypt_with_actual_key(datasync.oauth_unencrypted)
    else:
        encryptde_data = EncryptedData(
            key_id = datasync.crypto_key_id,
            key_kind = 'local_fernet',
            cypher_text = datasync.oauth_encrypted,
        )
        result = crypto.encrypt_with_actual_key(crypto.decrypt(encryptde_data))
    return result


async def main(*ars, **kwargs):
    parser = argparse.ArgumentParser(prog='rotate crypto token')
    parser.add_argument('--dry-run', action=argparse.BooleanOptionalAction, default=True)
    args = parser.parse_args()

    settings = from_granular_settings()
    crypto_keys_config = get_crypto_keys_config_from_env()
    crypto = CryptoController(crypto_keys_config)

    db = await create_engine(**settings.SQLA_DB_CFG_MASTER)

    async with db.acquire() as conn:
        async with conn.begin():
            datasyncs = await BIDatasyncCryptoManager(conn).get_objects(
                crypto_key_id=crypto_keys_config.actual_key_id,
            )

            for datasync in datasyncs:
                if args.dry_run:
                    LOGGER.info(f'datasync ID {datasync.id} will be modified')
                    continue
                data = get_crypto_data(crypto, datasync)
                await BIDatasyncCryptoManager(conn).update(
                    datasync.id,
                    oauth_encrypted=data.get('cypher_text'),
                    crypto_key_id=data.get('key_id'),
                )
                LOGGER.info(f'datasync ID {datasync.id} has been modified')

    db.close()
    await db.wait_closed()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
