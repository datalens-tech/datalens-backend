import attr

from dl_obfuscator.context import ObfuscationContext
from dl_obfuscator.obfuscators.base import BaseObfuscator
from dl_obfuscator.secret_keeper import SecretKeeper


@attr.s
class SecretObfuscator(BaseObfuscator):
    secret_keeper: SecretKeeper = attr.ib(repr=False)

    def obfuscate(self, text: str, context: ObfuscationContext) -> str:
        secrets = self.secret_keeper.get_secrets()

        for secret in secrets:
            text = text.replace(secret, f"***{secrets[secret] or 'hidden'}***")

        if context != ObfuscationContext.INSPECTOR:
            params = self.secret_keeper.get_params()
            for param in params:
                text = text.replace(param, f"***{params[param] or 'hidden'}***")

        return text
