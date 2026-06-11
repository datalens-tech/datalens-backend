import attr

from dl_obfuscator import (
    SecretKeeper,
    create_base_obfuscators,
    create_request_engine,
    get_secret_strings,
)
from dl_obfuscator.context import ObfuscationContext


@attr.s
class _AuthData:
    oauth_token: str = attr.ib(repr=False)


class TestPerRequestFullChain:
    def test_secrets_masked_in_all_contexts_params_preserved_in_inspector(self) -> None:
        base = create_base_obfuscators()
        keeper = SecretKeeper()

        # Pre-populate as middleware would.
        headers = {
            "Authorization": "Bearer header-secret-XXXX",
            "Cookie": "Session_id=cookie-secret-XXXX",
        }
        for hdr_name, hdr_value in headers.items():
            keeper.add_secret(hdr_value, f"header.{hdr_name}")

        keeper.add_secrets(get_secret_strings(_AuthData(oauth_token="oauth-secret-XXXX")), prefix="auth_data")

        # Pre-populate one param as endpoint would.
        keeper.add_param("param-input-XXXX", "user_param")

        engine = create_request_engine(base, secret_keeper=keeper)

        # Secrets must be masked in ALL contexts (including INSPECTOR).
        secret_string = (
            "Authorization=Bearer header-secret-XXXX Cookie:Session_id=cookie-secret-XXXX auth:oauth-secret-XXXX"
        )
        for ctx in [
            ObfuscationContext.LOGS,
            ObfuscationContext.SENTRY,
            ObfuscationContext.TRACING,
            ObfuscationContext.USAGE_TRACKING,
            ObfuscationContext.INSPECTOR,
        ]:
            out = engine.obfuscate(secret_string, ctx)
            assert "header-secret-XXXX" not in out, ctx
            assert "cookie-secret-XXXX" not in out, ctx
            assert "oauth-secret-XXXX" not in out, ctx

        # Params: masked in all NON-inspector contexts; preserved in INSPECTOR.
        param_string = "WHERE x = param-input-XXXX"
        for ctx in [
            ObfuscationContext.LOGS,
            ObfuscationContext.SENTRY,
            ObfuscationContext.TRACING,
            ObfuscationContext.USAGE_TRACKING,
        ]:
            out = engine.obfuscate(param_string, ctx)
            assert "param-input-XXXX" not in out, ctx

        inspector_out = engine.obfuscate(param_string, ObfuscationContext.INSPECTOR)
        assert "param-input-XXXX" in inspector_out
