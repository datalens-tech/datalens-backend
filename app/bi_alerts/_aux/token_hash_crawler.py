import asyncio
import argparse
import logging
import sys

from aiopg.sa import create_engine
from typing import Optional, List

from bi_alerts.settings import from_granular_settings
from bi_alerts.db_manager import DbManager
from bi_alerts.models.db import BIDatasync
from bi_alerts.utils.charts import hash_str


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
LOGGER = logging.getLogger(__name__)


class BIDatasyncCryptoManager(DbManager):
    _model = BIDatasync
    _whitelist = {
        'oauth_hash',
    }

    async def get_objects(
        self,
    ) -> Optional[List[BIDatasync]]:
        result = await self.db_conn.execute(
            self._model.select_().where(
                self._model.oauth_hash == None,
            )
        )
        return await result.fetchall()


async def main(*ars, **kwargs):
    parser = argparse.ArgumentParser(prog='generate hash token')
    parser.add_argument('--dry-run', action=argparse.BooleanOptionalAction, default=True)
    args = parser.parse_args()

    settings = from_granular_settings()
    db = await create_engine(**settings.SQLA_DB_CFG_MASTER)

    async with db.acquire() as conn:
        async with conn.begin():
            datasyncs = await BIDatasyncCryptoManager(conn).get_objects()

            for datasync in datasyncs:
                if args.dry_run:
                    LOGGER.info(f'datasync ID {datasync.id} will be modified')
                    continue
                oauth_hash = hash_str(datasync.oauth_unencrypted)
                await BIDatasyncCryptoManager(conn).update(
                    datasync.id,
                    oauth_hash=oauth_hash,
                )
                LOGGER.info(f'datasync ID {datasync.id} has been modified')

    db.close()
    await db.wait_closed()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
