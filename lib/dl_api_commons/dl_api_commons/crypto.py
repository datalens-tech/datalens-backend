import hashlib
import hmac


def get_hmac_hex_digest(target: bytes, secret_key: bytes) -> str:
    return hmac.new(key=secret_key, msg=target, digestmod=hashlib.sha256).hexdigest()
