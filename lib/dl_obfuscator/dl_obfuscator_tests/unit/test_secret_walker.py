import enum
import logging

import attr
import pydantic
import pytest

from dl_obfuscator import get_secret_strings
import dl_settings


class _SimpleSettings(pydantic.BaseModel):
    public: str
    secret: str = pydantic.Field(repr=False)
    maybe_secret: str | None = pydantic.Field(default=None, repr=False)


class TestStrAndNone:
    def test_str_secret_is_emitted(self) -> None:
        settings = _SimpleSettings(public="visible", secret="s3cret-value")
        assert get_secret_strings(settings) == {"secret": "s3cret-value"}

    def test_non_secret_str_is_not_emitted(self) -> None:
        settings = _SimpleSettings(public="visible", secret="s3cret-value")
        result = get_secret_strings(settings)
        assert len(result) == 1
        assert "secret" in result
        assert "public" not in result

    def test_optional_none_value_is_skipped(self) -> None:
        settings = _SimpleSettings(public="visible", secret="s3cret-value", maybe_secret=None)
        paths = get_secret_strings(settings)
        assert "maybe_secret" not in paths


class _TupleSettings(pydantic.BaseModel):
    tokens: tuple[str, ...] = pydantic.Field(repr=False)


class TestTuple:
    def test_tuple_of_str_emits_each_element(self) -> None:
        settings = _TupleSettings(tokens=("alpha", "beta", "gamma"))
        assert get_secret_strings(settings) == {
            "tokens[0]": "alpha",
            "tokens[1]": "beta",
            "tokens[2]": "gamma",
        }


class _ListSettings(pydantic.BaseModel):
    tokens: list[str] = pydantic.Field(repr=False)


class TestList:
    def test_list_of_str_emits_each_element(self) -> None:
        settings = _ListSettings(tokens=["alpha", "beta", "gamma"])
        assert get_secret_strings(settings) == {
            "tokens[0]": "alpha",
            "tokens[1]": "beta",
            "tokens[2]": "gamma",
        }


class _SetSettings(pydantic.BaseModel):
    tokens: set[str] = pydantic.Field(repr=False)


class _FrozensetSettings(pydantic.BaseModel):
    tokens: frozenset[str] = pydantic.Field(repr=False)


class TestSetAndFrozenset:
    def test_set_of_str_emits_each_value(self) -> None:
        settings = _SetSettings(tokens={"alpha", "beta", "gamma"})
        emitted = get_secret_strings(settings)
        assert set(emitted.keys()) == {"tokens[0]", "tokens[1]", "tokens[2]"}
        assert set(emitted.values()) == {"alpha", "beta", "gamma"}

    def test_frozenset_of_str_emits_each_value(self) -> None:
        settings = _FrozensetSettings(tokens=frozenset({"alpha", "beta", "gamma"}))
        emitted = get_secret_strings(settings)
        assert set(emitted.keys()) == {"tokens[0]", "tokens[1]", "tokens[2]"}
        assert set(emitted.values()) == {"alpha", "beta", "gamma"}


class _DictStrSettings(pydantic.BaseModel):
    keymap: dict[str, str] = pydantic.Field(repr=False)


class _Client(pydantic.BaseModel):
    public_id: str
    client_secret: str = pydantic.Field(repr=False)


class _DictModelSettings(pydantic.BaseModel):
    clients: dict[str, _Client] = pydantic.Field(default_factory=dict)


class TestDict:
    def test_dict_of_str_emits_each_value(self) -> None:
        settings = _DictStrSettings(keymap={"k1": "v1", "k2": "v2"})
        assert get_secret_strings(settings) == {
            "keymap[k1]": "v1",
            "keymap[k2]": "v2",
        }

    def test_dict_of_models_emits_only_inner_secrets(self) -> None:
        settings = _DictModelSettings(
            clients={
                "google": _Client(public_id="g-id", client_secret="g-secret"),
                "yandex": _Client(public_id="y-id", client_secret="y-secret"),
            },
        )
        assert get_secret_strings(settings) == {
            "clients[google].client_secret": "g-secret",
            "clients[yandex].client_secret": "y-secret",
        }


class _BytesSettings(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)
    hmac_key: bytes = pydantic.Field(repr=False)


class TestBytes:
    def test_ascii_bytes_decoded(self) -> None:
        settings = _BytesSettings(hmac_key=b"signing-key")
        assert get_secret_strings(settings) == {"hmac_key": "signing-key"}

    def test_invalid_utf8_replaced_not_crashed(self) -> None:
        settings = _BytesSettings(hmac_key=b"\xff\xfe\xfd")
        result = get_secret_strings(settings)
        assert len(result) == 1
        assert result["hmac_key"] == "\ufffd\ufffd\ufffd"


@attr.s(frozen=True)
class _LegacyChild:
    password: str = attr.ib(repr=False)
    public: str = attr.ib()


@attr.s(frozen=True)
class _LegacyRoot:
    master_token: str = attr.ib(repr=False)
    nested: _LegacyChild = attr.ib()


@attr.s(slots=True, frozen=True)
class _LegacySlots:
    token: str = attr.ib(repr=False)


class TestAttrs:
    def test_attrs_root_emits_repr_false_field(self) -> None:
        root = _LegacyRoot(
            master_token="root-tok",
            nested=_LegacyChild(password="pw", public="visible"),
        )
        assert get_secret_strings(root) == {
            "master_token": "root-tok",
            "nested.password": "pw",
        }

    def test_attrs_with_slots(self) -> None:
        inst = _LegacySlots(token="t")
        assert get_secret_strings(inst) == {"token": "t"}


@attr.s(frozen=True)
class _LegacyFallback:
    US_MASTER_TOKEN: str = attr.ib(repr=False)
    PUBLIC_HOST: str = attr.ib()


class _PydanticRoot(dl_settings.BaseRootSettingsWithFallback):
    OBFUSCATION_ENABLED: bool = False
    AUTH_CLIENT_SECRET: str | None = pydantic.Field(default=None, repr=False)


class TestFallbackBridge:
    def test_pydantic_root_walks_into_attrs_fallback(self) -> None:
        legacy = _LegacyFallback(US_MASTER_TOKEN="legacy-tok", PUBLIC_HOST="visible.example")
        root = _PydanticRoot(fallback=legacy, AUTH_CLIENT_SECRET="zitadel-secret")
        emitted = get_secret_strings(root)
        assert emitted == {
            "AUTH_CLIENT_SECRET": "zitadel-secret",
            "fallback.US_MASTER_TOKEN": "legacy-tok",
        }


class _Inner(pydantic.BaseModel):
    val_a: str
    val_b: str


class _PropagationParent(pydantic.BaseModel):
    inner: _Inner = pydantic.Field(repr=False)


class _Color(enum.Enum):
    RED = "red"


class _PrimitiveSettings(pydantic.BaseModel):
    count: int = pydantic.Field(repr=False)
    flag: bool = pydantic.Field(repr=False)
    ratio: float = pydantic.Field(repr=False)
    color: _Color = pydantic.Field(repr=False)


class _EmptySettings(pydantic.BaseModel):
    pass


class _PlainPydanticRoot(dl_settings.BaseRootSettings):
    OBFUSCATION_ENABLED: bool = False
    MASTER_TOKEN: str | None = pydantic.Field(default=None, repr=False)


class TestEdgeCases:
    def test_in_secret_propagates_into_nested_non_marked_fields(self) -> None:
        settings = _PropagationParent(inner=_Inner(val_a="x", val_b="y"))
        assert get_secret_strings(settings) == {
            "inner.val_a": "x",
            "inner.val_b": "y",
        }

    def test_non_str_primitives_ignored(self) -> None:
        settings = _PrimitiveSettings(count=7, flag=True, ratio=1.5, color=_Color.RED)
        assert get_secret_strings(settings) == {}

    def test_empty_settings_yields_nothing(self) -> None:
        assert get_secret_strings(_EmptySettings()) == {}

    def test_baseroot_without_fallback_works(self) -> None:
        settings = _PlainPydanticRoot(MASTER_TOKEN="tok")
        assert get_secret_strings(settings) == {"MASTER_TOKEN": "tok"}


class _Opaque:
    def __init__(self, payload: str) -> None:
        self.payload = payload


class _UnsupportedContainerSettings(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)
    blob: _Opaque = pydantic.Field(repr=False)


class _UnsupportedContainerPublicSettings(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)
    blob: _Opaque


class TestUnsupportedTypes:
    def test_unsupported_inside_secret_warns_and_emits_nothing(self, caplog: pytest.LogCaptureFixture) -> None:
        settings = _UnsupportedContainerSettings(blob=_Opaque(payload="leak-me"))
        with caplog.at_level(logging.WARNING, logger="dl_obfuscator.secret_walker"):
            emitted = get_secret_strings(settings)
        assert emitted == {}
        assert any(
            "skipping unsupported value at 'blob'" in record.message and record.levelno == logging.WARNING
            for record in caplog.records
        )

    def test_unsupported_outside_secret_does_not_warn(self, caplog: pytest.LogCaptureFixture) -> None:
        settings = _UnsupportedContainerPublicSettings(blob=_Opaque(payload="public"))
        with caplog.at_level(logging.WARNING, logger="dl_obfuscator.secret_walker"):
            emitted = get_secret_strings(settings)
        assert emitted == {}
        assert not any(record.levelno >= logging.WARNING for record in caplog.records)


@attr.s(frozen=True)
class _IntegrationLegacyRedis:
    PASSWORD: str = attr.ib(repr=False)
    HOST: str = attr.ib()


@attr.s(frozen=True)
class _IntegrationLegacyRoot:
    US_MASTER_TOKEN: str = attr.ib(repr=False)
    CSRF_SECRET: tuple[str, ...] = attr.ib(repr=False)
    CRYPTO_MAP: dict[str, str] = attr.ib(repr=False)
    HMAC_KEY: bytes = attr.ib(repr=False)
    CACHES_REDIS: _IntegrationLegacyRedis = attr.ib()
    PUBLIC_HOST: str = attr.ib()


class _IntegrationAuth(pydantic.BaseModel):
    CLIENT_SECRET: str = pydantic.Field(repr=False)
    CLIENT_ID: str


class _IntegrationPydanticRoot(dl_settings.BaseRootSettingsWithFallback):
    OBFUSCATION_ENABLED: bool = False
    AUTH: _IntegrationAuth | None = None


class TestIntegrationShape:
    def test_walker_finds_every_secret_in_real_shape(self) -> None:
        legacy = _IntegrationLegacyRoot(
            US_MASTER_TOKEN="us-tok",
            CSRF_SECRET=("csrf-a", "csrf-b"),
            CRYPTO_MAP={"k1": "key-one", "k2": "key-two"},
            HMAC_KEY=b"hmac-bytes",
            CACHES_REDIS=_IntegrationLegacyRedis(PASSWORD="redis-pw", HOST="visible.redis"),
            PUBLIC_HOST="public.example",
        )
        root = _IntegrationPydanticRoot(
            fallback=legacy,
            AUTH=_IntegrationAuth(CLIENT_SECRET="auth-secret", CLIENT_ID="public-client-id"),
        )

        emitted = get_secret_strings(root)

        assert emitted == {
            "AUTH.CLIENT_SECRET": "auth-secret",
            "fallback.US_MASTER_TOKEN": "us-tok",
            "fallback.CSRF_SECRET[0]": "csrf-a",
            "fallback.CSRF_SECRET[1]": "csrf-b",
            "fallback.CRYPTO_MAP[k1]": "key-one",
            "fallback.CRYPTO_MAP[k2]": "key-two",
            "fallback.HMAC_KEY": "hmac-bytes",
            "fallback.CACHES_REDIS.PASSWORD": "redis-pw",
        }


@attr.s(frozen=True)
class _AttrsCallableRepr:
    password: str = attr.ib(repr=lambda _v: "***")
    public: str = attr.ib(default="")


@attr.s(frozen=True)
class _AttrsOpaqueSecret:
    blob: _Opaque = attr.ib(repr=False)


@attr.s(frozen=True)
class _AttrsDataModelLike:
    password: str = attr.ib(default="")  # plain — no repr suppression
    public: str = attr.ib(default="")


def _attrs_password_only(cls: type) -> frozenset[str]:
    if cls is _AttrsDataModelLike:
        return frozenset({"password"})
    return frozenset()


class TestExtraSecretFieldsResolver:
    def test_resolver_picks_up_unmarked_field(self) -> None:
        inst = _AttrsDataModelLike(password="pw", public="visible")
        assert get_secret_strings(inst, extra_secret_fields=_attrs_password_only) == {"password": "pw"}

    def test_default_no_resolver_emits_nothing_for_unmarked(self) -> None:
        inst = _AttrsDataModelLike(password="pw", public="visible")
        assert get_secret_strings(inst) == {}


class TestAttrsReprCallable:
    def test_callable_repr_treated_as_secret(self) -> None:
        inst = _AttrsCallableRepr(password="pw", public="visible")
        assert get_secret_strings(inst) == {"password": "pw"}


class TestAttrsUnsupportedWarns:
    def test_attrs_unsupported_inside_secret_warns_and_emits_nothing(self, caplog: pytest.LogCaptureFixture) -> None:
        inst = _AttrsOpaqueSecret(blob=_Opaque(payload="leak-me"))
        with caplog.at_level(logging.WARNING, logger="dl_obfuscator.secret_walker"):
            emitted = get_secret_strings(inst)
        assert emitted == {}
        assert any(
            "skipping unsupported value at 'blob'" in record.message and record.levelno == logging.WARNING
            for record in caplog.records
        )
