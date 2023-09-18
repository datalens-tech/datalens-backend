from typing import (
    ClassVar,
    Sequence,
)


class ExternalAPIException(Exception):
    CODE: ClassVar[str] = "EXT_API"

    @classmethod
    def get_code_chain(cls) -> Sequence[str]:
        mro_codes = [clz.CODE for clz in cls.mro() if issubclass(clz, ExternalAPIException)]
        compressed_codes: list[str] = []
        for code in mro_codes:
            if len(compressed_codes) < 1 or compressed_codes[-1] != code:
                compressed_codes.append(code)
        return tuple(reversed(compressed_codes))
