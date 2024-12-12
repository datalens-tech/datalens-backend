import datetime
import typing

import attr
import jwt
import pydantic

import dl_auth_native.exc as dl_auth_native_exc


@attr.s(frozen=True)
class Payload:
    user_id: str = attr.ib()
    expires_at: datetime.datetime = attr.ib()


class PayloadSchema(pydantic.BaseModel):
    user_id: str = pydantic.Field(alias="userId")
    exp: int

    def to_dataclass(self) -> Payload:
        return Payload(
            user_id=self.user_id,
            expires_at=datetime.datetime.fromtimestamp(self.exp),
        )


class TokenError(dl_auth_native_exc.BaseError):
    pass


class DecodeError(TokenError):
    pass


class ValidationError(TokenError):
    pass


class DecoderProtocol(typing.Protocol):
    def decode(self, token: str) -> Payload:
        ...


@attr.s(frozen=True)
class Decoder:
    _key: str = attr.ib()
    _algorithms: list[str] = attr.ib()

    def decode(self, token: str) -> Payload:
        try:
            raw_payload = jwt.decode(jwt=token, key=self._key, algorithms=self._algorithms)
        except jwt.exceptions.InvalidSignatureError as exc:
            raise DecodeError("Invalid signature") from exc
        except jwt.exceptions.ExpiredSignatureError as exc:
            raise DecodeError("Expired signature") from exc
        except jwt.exceptions.InvalidTokenError as exc:
            raise DecodeError("Invalid token") from exc

        try:
            payload = PayloadSchema(**raw_payload)
        except pydantic.ValidationError as exc:
            raise ValidationError("Invalid payload") from exc

        return payload.to_dataclass()


__all__ = [
    "Decoder",
    "DecoderProtocol",
    "Payload",
    "TokenError",
    "DecodeError",
    "ValidationError",
]
