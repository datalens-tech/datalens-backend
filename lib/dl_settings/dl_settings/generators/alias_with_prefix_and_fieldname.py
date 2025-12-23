import typing

import pydantic


def prefix_alias_generator(prefixes: list[str]) -> pydantic.AliasGenerator:
    def create_prefix_function(prefixes: list[str]) -> typing.Callable[[str], str]:
        def with_deprecated_prefix_and_fieldname(string: str) -> typing.Any:
            aliases = [prefix + string for prefix in prefixes]
            return pydantic.AliasChoices(string, *aliases)

        return with_deprecated_prefix_and_fieldname

    return pydantic.AliasGenerator(
        validation_alias=create_prefix_function(prefixes),
    )
