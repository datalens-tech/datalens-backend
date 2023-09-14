from __future__ import annotations

import hashlib
import hmac
from typing import Union


def get_hmac_hex_digest(target: Union[bytes, str], secret_key: bytes) -> str:
    return hmac.new(
        key=secret_key, msg=target if isinstance(target, bytes) else target.encode(), digestmod=hashlib.sha256
    ).hexdigest()
