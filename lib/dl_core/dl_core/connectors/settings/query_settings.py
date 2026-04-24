import logging

import pydantic
from typing_extensions import Self

import dl_settings


LOGGER = logging.getLogger(__name__)


class QuerySettingsSettings(dl_settings.BaseSettings):
    ENABLED: bool = False
    # Whitelist of allowed setting names: None = unrestricted, empty = all forbidden, populated = allow-list.
    # Default is empty (locked down); connectors opt in by overriding to `None` or a curated set.
    ALLOWED: frozenset[str] | None = frozenset()
    # Names that DataLens itself manages and the user must not shadow.
    FORBIDDEN: frozenset[str] = frozenset()

    @pydantic.model_validator(mode="after")
    def _warn_on_dropped_forbidden_defaults(self) -> Self:
        """Warn if a deployment override removed any names from the class's default `FORBIDDEN` set.

        These defaults exist for a reason — DataLens may rely on them. Override at your own risk.
        """
        field_default = type(self).model_fields["FORBIDDEN"].default
        if not isinstance(field_default, (frozenset, set)):
            return self
        dropped = field_default - self.FORBIDDEN
        if dropped:
            LOGGER.warning(
                "%s.FORBIDDEN dropped entries %s from the class default; "
                "these defaults are present for a reason — overwrite with extreme caution at your own risk, "
                "the connector may misbehave or break.",
                type(self).__name__,
                sorted(dropped),
            )
        return self
