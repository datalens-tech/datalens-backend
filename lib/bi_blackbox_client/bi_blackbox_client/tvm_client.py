import logging
import os

LOGGER = logging.getLogger(__name__)


def _env_tvm_info():  # type: ignore  # TODO: fix
    """
    Get `(env_name, tvm_client_id, tvm_secret)` from environment.
    """
    result = os.environ.get("TVM_INFO")
    if not result:
        return None
    result = result.split()  # type: ignore  # TODO: fix
    if len(result) != 3:
        LOGGER.warning("Malformed TVM_INFO")
    return tuple(result)


TVM_INFO = _env_tvm_info()
TVM_CLIENTS = {}  # type: ignore  # TODO: fix  # singleton memo: tvm_info -> (tvm_client, blackbox_client_id)
DEFAULT_BLACKBOX_NAME = "ProdYateam"


def get_tvm_client(tvm_info=None, blackbox_client_id_name=None, require=True):  # type: ignore  # TODO: fix
    """
    FIXME: Deprecated. Use `bi_blackbox_client.tvm`
    """
    # Defaults
    if not tvm_info:
        tvm_info = TVM_INFO
    elif isinstance(tvm_info, str):
        tvm_info = tuple(tvm_info.split())
    if not tvm_info:
        if require:
            raise Exception("No TVM_INFO available")
        LOGGER.warning("No TVM_INFO available")
        return None, None

    if blackbox_client_id_name is None:
        blackbox_client_id_name = DEFAULT_BLACKBOX_NAME

    # Cache
    result = TVM_CLIENTS.get(tvm_info)
    if result is not None:
        return result

    # Import on-demand to make it more optional
    import tvm2

    # Enum that matches `BLACKBOX_MAP` in the same module:
    from tvm2.protocol import BlackboxClientId

    # ...
    blackbox_client_id_obj = getattr(BlackboxClientId, blackbox_client_id_name)
    blackbox_client_id = blackbox_client_id_obj.value
    _, tvm_client_id, tvm_secret = tvm_info
    # Intentially not supporting: qloud tvm2 variant.
    tvm_client = tvm2.TVM2(
        client_id=tvm_client_id,
        blackbox_client=blackbox_client_id_obj,
        secret=tvm_secret,
    )
    # Also return the blackbox id as it is often necessary.
    result = tvm_client, blackbox_client_id

    # Cache
    TVM_CLIENTS[tvm_info] = result

    return result


def get_tvm_headers(  # type: ignore  # TODO: fix
    tvm_info=None,
    blackbox_client_id_name=None,
    destination_client_id=None,
    destination_title=None,
    attempts=3,
    require=True,
) -> dict:
    """
    FIXME: Deprecated. Use `bi_blackbox_client.tvm`
    """
    # reference:
    # https://a.yandex-team.ru/arc/trunk/arcadia/library/python/blackbox/blackbox.py?rev=6126855#L308
    tvm_client, blackbox_client_id = get_tvm_client(
        tvm_info=tvm_info, blackbox_client_id_name=blackbox_client_id_name, require=require
    )
    if tvm_client is None:
        LOGGER.warning("No tvm_client, returning empty TVM headers")
        return {}
    if destination_client_id is None:
        destination_title = destination_title or "blackbox {}".format(blackbox_client_id_name or DEFAULT_BLACKBOX_NAME)
        destination_client_id = blackbox_client_id

    tickets = None
    for _ in range(attempts):
        tickets = tvm_client.get_service_tickets(destination_client_id)
        ticket = tickets.get(destination_client_id)
        if ticket:
            return {"X-Ya-Service-Ticket": ticket}
    LOGGER.error(
        "Could not get service ticket for %r (%r) (attempts=%r, service_tickets=%r)",
        destination_client_id,
        destination_title,
        attempts,
        tickets,
    )
    return {}
