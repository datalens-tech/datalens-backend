from __future__ import annotations

from os import environ
from typing import Optional


try:
    from library.python.vault_client.instances import Production as VaultClient  # type: ignore  # noqa
except ImportError:
    from vault_client.instances import Production as VaultClient  # type: ignore  # noqa


def get_secret(secret_id: str, use_ssh_auth: bool = False, yav_token: Optional[str] = None) -> dict[str, str]:
    """
    Currently we don't want to share ext testing secrets with all Arcadia users.
    So we will use SSH agent to fetch ext testing secrets with SSH creds of users who launching test.
    In-Docker launch notes:
    Expected that SSH agent socket mounted to container and username of agent owner stored in environ['YAV_USER']
    """
    if use_ssh_auth:
        username = environ.get("YAV_USER")

        vc = VaultClient(
            check_status=False,
            decode_files=True,
            rsa_login=username,
        )
    else:
        assert yav_token is not None, "For not-ssh auth in vault client, YAV token should be provided."
        vc = VaultClient(
            check_status=False,
            decode_files=True,
            authorization=yav_token,
        )

    sec_data = vc.get_version(secret_id)
    # sec_data = {"created_at": ..., "value": {...}}
    data = sec_data["value"]
    return data
