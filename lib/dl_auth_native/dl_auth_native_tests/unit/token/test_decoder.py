import datetime
import typing

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import jwt
import pytest

import dl_auth_native.token as token


@pytest.fixture(name="algorithm")
def fixture_algorithm() -> str:
    return "RS256"


@pytest.fixture(name="private_key")
def fixture_private_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=4096)


@pytest.fixture(name="public_key")
def fixture_public_key(private_key: rsa.RSAPrivateKey) -> rsa.RSAPublicKey:
    return private_key.public_key()


@pytest.fixture(name="private_key_pem")
def fixture_private_key_pem(private_key: rsa.RSAPrivateKey) -> str:
    private_key_bytes = private_key.private_bytes(  # type: ignore # incomplete stubs
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    return private_key_bytes.decode()


@pytest.fixture(name="public_key_pem")
def fixture_public_key_pem(public_key: rsa.RSAPublicKey) -> str:
    public_key_bytes = public_key.public_bytes(
        encoding=serialization.Encoding.PEM, format=serialization.PublicFormat.SubjectPublicKeyInfo
    )

    return public_key_bytes.decode()


@pytest.fixture(name="another_private_key_pem")
def fixture_another_public_key_pem() -> str:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=4096)
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return private_key_bytes.decode()


class Encoder(typing.Protocol):
    def __call__(self, payload: dict) -> str:
        ...


@pytest.fixture(name="encoder")
def fixture_encoder(private_key_pem: str, algorithm: str) -> Encoder:
    def inner(payload: dict) -> str:
        return jwt.encode(payload, private_key_pem, algorithm=algorithm)

    return inner


@pytest.fixture(name="decoder")
def fixture_decoder(public_key_pem: str, algorithm: str) -> token.Decoder:
    return token.Decoder(key=public_key_pem, algorithms=[algorithm])


def test_decode_default(
    encoder: Encoder,
    decoder: token.Decoder,
) -> None:
    expires_at = datetime.datetime.now().replace(microsecond=0) + datetime.timedelta(seconds=60)
    user_id = "test-user-id"

    raw_payload = {
        "userId": user_id,
        "exp": expires_at.timestamp(),
    }

    encoded = encoder(raw_payload)
    payload = decoder.decode(encoded)

    assert payload == token.Payload(user_id=user_id, expires_at=expires_at)


def test_decode_invalid_key(
    decoder: token.Decoder,
    another_private_key_pem: str,
    algorithm: str,
) -> None:
    raw_payload = {
        "userId": "test-user-id",
        "exp": datetime.datetime.now().timestamp(),
    }

    encoded = jwt.encode(raw_payload, another_private_key_pem, algorithm=algorithm)

    with pytest.raises(token.DecodeError) as exc_info:
        decoder.decode(encoded)

    assert exc_info.value.message == "Invalid signature"


def test_decode_invalid_token(
    decoder: token.Decoder,
) -> None:
    with pytest.raises(token.DecodeError) as exc_info:
        decoder.decode("invalid-token")

    assert exc_info.value.message == "Invalid token"


def test_decode_expired(
    encoder: Encoder,
    decoder: token.Decoder,
) -> None:
    expires_at = datetime.datetime.now().replace(microsecond=0) - datetime.timedelta(seconds=60)
    user_id = "test-user-id"

    raw_payload = {
        "userId": user_id,
        "exp": expires_at.timestamp(),
    }

    encoded = encoder(raw_payload)

    with pytest.raises(token.DecodeError) as exc_info:
        decoder.decode(encoded)

    assert exc_info.value.message == "Expired signature"


def test_decode_invalid_payload(
    encoder: Encoder,
    decoder: token.Decoder,
) -> None:
    raw_payload = {
        "userId": "test-user-id",
        "exp": datetime.datetime.now().timestamp() + 60,
    }

    encoded = encoder(raw_payload)

    with pytest.raises(token.ValidationError) as exc_info:
        decoder.decode(encoded)

    assert exc_info.value.message == "Invalid payload"
