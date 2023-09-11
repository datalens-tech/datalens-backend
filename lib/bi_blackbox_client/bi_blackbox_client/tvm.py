import asyncio
import hashlib
import json
import logging
import os
import time
from concurrent.futures.thread import ThreadPoolExecutor
from typing import ClassVar, Dict, FrozenSet, Optional, Tuple

from bi_defaults.environments import TvmDestination

import tvmauth


LOGGER = logging.getLogger(__name__)

TVM_SERVICE_TICKET_HEADER = 'X-Ya-Service-Ticket'
TVM_AUTO_CACHE_DIR_PREFIX = '/tmp/tvmauth/cache_'
TVM_AUTO_CACHE_DIR_HASH_CHARS = 22


def get_tvm_info_and_secret_from_env(key: str = 'TVM_INFO') -> Optional[Tuple[str, str, str]]:
    """
    Get `(env_name, tvm_client_id, tvm_secret)` from environment.
    """
    tvm_info_and_secret_str = os.environ.get(key)
    if not tvm_info_and_secret_str:
        return None
    tvm_info_and_secret = tvm_info_and_secret_str.split()
    if len(tvm_info_and_secret) != 3:
        LOGGER.error('Malformed TVM_INFO env')
        return None
    tvm_name, tvm_id, tvm_secret = tvm_info_and_secret
    return tvm_name, tvm_id, tvm_secret


TvmDestinationSet = FrozenSet[TvmDestination]
DEFAULT_TVM_DESTINATIONS: TvmDestinationSet = frozenset((TvmDestination.BlackboxProdYateam,))


def make_tvm_client(
        tvm_info_and_secret: Tuple[str, str, str],
        destinations: TvmDestinationSet = DEFAULT_TVM_DESTINATIONS,
        enable_service_ticket_checking: bool = False,
        enable_user_ticket_checking: Optional[tvmauth.BlackboxEnv] = None,
        max_tries: int = 3,
        disk_cache_dir: Optional[str] = None,
        auto_cache_dir: bool = True,
        auto_cache_dir_prefix: str = TVM_AUTO_CACHE_DIR_PREFIX,
        auto_cache_dir_hash_chars: int = TVM_AUTO_CACHE_DIR_HASH_CHARS,
) -> tvmauth.TvmClient:
    """
    Usage notes:

    Don't forget to `.close()` the TvmClient if it is not going to be used anymore.

    TvmClient does not allow requesting service tickets for destinations not
    specified on instantiation. For that, might as well instantiate a new one
    (and close it afterwards).

    TvmClient instantiation is synchronous (networked), ticket-creation is fast.

    TvmClient does not survive forking, so a singleton should not be instantiated at import-time.
    """
    name, tvm_id_s, tvm_secret = tvm_info_and_secret
    tvm_id = int(tvm_id_s)

    if disk_cache_dir is None and auto_cache_dir:
        params_hashable = (
            tvm_id,
            tuple(dst.value for dst in sorted(destinations)),
            enable_service_ticket_checking,
            enable_user_ticket_checking,
        )
        params_hashable_str = json.dumps(params_hashable)
        params_hash = hashlib.sha256(params_hashable_str.encode()).hexdigest()
        params_hash = params_hash[:auto_cache_dir_hash_chars]
        disk_cache_dir = auto_cache_dir_prefix + params_hash
        os.makedirs(disk_cache_dir, mode=0o700, exist_ok=True)

    tvm_cfg = tvmauth.TvmApiClientSettings(
        self_tvm_id=tvm_id,
        self_secret=tvm_secret,
        dsts={dst.name: dst.value for dst in sorted(destinations)},
        enable_service_ticket_checking=enable_service_ticket_checking,
        enable_user_ticket_checking=enable_user_ticket_checking,
        disk_cache_dir=disk_cache_dir,
    )

    last_exc = None
    for attempt in range(max_tries):
        try:
            tvm_cli = tvmauth.TvmClient(tvm_cfg)
            return tvm_cli
        except tvmauth.exceptions.RetriableException as exc:
            last_exc = exc
            if attempt != max_tries - 1:
                time.sleep(0.1)
            continue
    raise Exception(
        f"TvmClient instantiation raised a retriable error in all {max_tries} tries",
    ) from last_exc


class TvmCliSingleton:
    """
    Singleton manager.

    Make a subclass if parameters (such as `destinations`) need to be changed.
    """

    # Cached client
    tvm_cli: ClassVar[Optional[tvmauth.TvmClient]] = None
    # Default parameters
    tvm_info_and_secret: ClassVar[Optional[Tuple[str, str, str]]] = get_tvm_info_and_secret_from_env()
    destinations: ClassVar[TvmDestinationSet] = DEFAULT_TVM_DESTINATIONS
    enable_service_ticket_checking: ClassVar[bool] = False
    enable_user_ticket_checking: ClassVar[Optional[tvmauth.BlackboxEnv]] = None
    disk_cache_dir: ClassVar[Optional[str]] = None
    auto_cache_dir: ClassVar[bool] = True

    @classmethod
    def get_tvm_cli_sync(cls) -> tvmauth.TvmClient:
        tvm_cli = cls.tvm_cli
        if tvm_cli is not None:
            return tvm_cli

        tvm_info_and_secret = cls.tvm_info_and_secret or get_tvm_info_and_secret_from_env()
        if tvm_info_and_secret is None:
            raise Exception("No TVM_INFO available")
        tvm_cli = make_tvm_client(
            tvm_info_and_secret=tvm_info_and_secret,
            destinations=cls.destinations,
            enable_service_ticket_checking=cls.enable_service_ticket_checking,
            enable_user_ticket_checking=cls.enable_user_ticket_checking,
            disk_cache_dir=cls.disk_cache_dir,
            auto_cache_dir=cls.auto_cache_dir,
        )
        cls.tvm_cli = tvm_cli
        return tvm_cli

    @classmethod
    async def get_tvm_cli(cls) -> tvmauth.TvmClient:
        """
        To simplify configuration, allow tvmcli creation ondemand (in TPE).
        """
        tvm_cli = cls.tvm_cli
        if tvm_cli is not None:
            return tvm_cli

        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor(thread_name_prefix="TVMAUTH_TPE_") as tpe:
            tvm_cli = await loop.run_in_executor(tpe, cls.get_tvm_cli_sync)
        return tvm_cli  # type: ignore


def get_tvm_service_ticket(
        tvm_cli: tvmauth.TvmClient,
        destination: TvmDestination,
        check_status: bool = True,
) -> str:
    result: str = tvm_cli.get_service_ticket_for(destination.name)
    if check_status:
        # Minimal status check. See also:
        # https://a.yandex-team.ru/arc/trunk/arcadia/library/python/tvmauth#tvmclient
        status = tvm_cli.status
        if status.code != tvmauth.TvmClientStatus.Ok:
            LOGGER.error(f"tvm_cli status is not ok: {status=!r}; {status.code=!r}; {status.last_error=!r}")
    return result


def get_tvm_headers(
        tvm_cli: tvmauth.TvmClient,
        destination: TvmDestination,
        check_status: bool = True,
) -> Dict[str, str]:
    tvm_secret_service_ticket = get_tvm_service_ticket(
        tvm_cli=tvm_cli,
        destination=destination,
        check_status=check_status,
    )
    return {TVM_SERVICE_TICKET_HEADER: tvm_secret_service_ticket}
