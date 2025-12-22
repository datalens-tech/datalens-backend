import typing

import pydantic


def create_prefix_function(prefix: str) -> typing.Callable[[str], str]:
    def with_deprecated_prefix_and_fieldname(string: str) -> typing.Any:
        return pydantic.AliasChoices(string, prefix + string)

    return with_deprecated_prefix_and_fieldname


def prefix_and_fieldname_alias_generator(prefix: str) -> pydantic.AliasGenerator:
    return pydantic.AliasGenerator(
        validation_alias=create_prefix_function(prefix),
    )
