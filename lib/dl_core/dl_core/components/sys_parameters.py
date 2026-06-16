"""Registry of system (``_sys.*``) dataset parameters.

A system parameter is an ordinary dataset parameter whose name (``field.title``)
starts with the reserved ``_sys.`` prefix. Its value is resolved on the server
from the current request rather than supplied by the client. The registry maps
each system parameter name to a resolver and its expected type; the value source
is the resolver's concern and is intentionally not tied to any single object
(``_sys.user_id`` reads from the request context, future entries may read
elsewhere).
"""

from collections.abc import Callable

import attr

from dl_api_commons.base_models import RequestContextInfo
from dl_constants import UserDataType

SYS_PARAMETER_PREFIX = "_sys."

# A resolver produces the parameter value for the current request.
SysParameterResolver = Callable[[RequestContextInfo], str | None]


@attr.s(frozen=True)
class SysParameter:
    resolver: SysParameterResolver = attr.ib()
    expected_type: UserDataType = attr.ib()


def _resolve_user_id(rci: RequestContextInfo) -> str | None:
    return rci.user_id


SYS_PARAMETERS: dict[str, SysParameter] = {
    "_sys.user_id": SysParameter(resolver=_resolve_user_id, expected_type=UserDataType.string),
}


def is_sys_name(name: str) -> bool:
    """Whether ``name`` falls under the reserved ``_sys.`` namespace."""
    return name.startswith(SYS_PARAMETER_PREFIX)


def is_registered_sys_name(name: str) -> bool:
    """Whether ``name`` is a known system parameter (registered)."""
    return name in SYS_PARAMETERS


def resolve_sys_value(name: str, rci: RequestContextInfo) -> str | None:
    """Resolve the value of a registered system parameter for the request."""
    return SYS_PARAMETERS[name].resolver(rci)


def get_sys_parameter_expected_type(name: str) -> UserDataType:
    """The cast a registered system parameter must declare."""
    return SYS_PARAMETERS[name].expected_type
