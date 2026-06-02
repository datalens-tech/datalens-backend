from typing import cast

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import pytest


@pytest.fixture(scope="session")
def private_key() -> str:
    key_obj = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key_obj.private_bytes(
        # casts work around stale cryptography-stubs typing enum members as str
        encoding=cast(serialization.Encoding, serialization.Encoding.PEM),
        format=cast(serialization.PrivateFormat, serialization.PrivateFormat.PKCS8),
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()


@pytest.fixture(scope="session")
def public_key(private_key: str) -> str:
    loaded = serialization.load_pem_private_key(private_key.encode(), password=None)
    return (
        loaded.public_key()
        .public_bytes(
            encoding=cast(serialization.Encoding, serialization.Encoding.PEM),
            format=cast(serialization.PublicFormat, serialization.PublicFormat.SubjectPublicKeyInfo),
        )
        .decode()
    )
