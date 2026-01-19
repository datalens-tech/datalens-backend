import attr

from dl_obfuscator.context import ObfuscationContext
from dl_obfuscator.obfuscators.base import BaseObfuscator
from dl_obfuscator.secret_keeper import SecretKeeper


@attr.s
class SecretObfuscator(BaseObfuscator):
    secret_keeper: SecretKeeper = attr.ib(repr=False)

    def obfuscate(self, text: str, context: ObfuscationContext) -> str:
        secrets = list(self.secret_keeper.get_secrets().items())
        if context != ObfuscationContext.INSPECTOR:
            secrets.extend(self.secret_keeper.get_params().items())

        for secret, replacement in secrets:
            replacement = replacement or "hidden"
            replacement = f"***{replacement}***"
            text = text.replace(secret, replacement)

        return text
