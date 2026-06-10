"""Helpers for resolving system (``_sys.*``) parameter values into query specs.

System parameters are server-resolved (see ``dl_core.components.sys_parameters``).
This module bridges the registry to the data-API query pipeline: it rejects any
client-supplied value for a ``_sys.*`` parameter and injects the server-resolved
value for every registered system parameter present in the dataset.
"""

from dl_api_commons.base_models import RequestContextInfo
from dl_constants.enums import CalcMode
from dl_core.components.sys_parameters import (
    is_registered_sys_name,
    is_sys_name,
    resolve_sys_value,
)
from dl_core.exc import ParameterValueSystemParameterNotSettableError
from dl_core.fields import ResultSchema
from dl_query_processing.compilation.specs import ParameterValueSpec


def resolve_sys_parameter_value_specs(
    parameter_value_specs: list[ParameterValueSpec],
    result_schema: ResultSchema,
    rci: RequestContextInfo,
) -> list[ParameterValueSpec]:
    """Reject client-supplied ``_sys.*`` values and inject server-resolved ones.

    The server is the sole source of a system parameter's value: a client-supplied
    value raises. The resolved value is appended for every registered ``_sys.*``
    parameter in the dataset; a ``None`` resolution is omitted so the ordinary
    ``default_value`` fallback applies.
    """
    result: list[ParameterValueSpec] = []
    for spec in parameter_value_specs:
        title = result_schema.by_guid(spec.field_id).title
        if is_sys_name(title):
            raise ParameterValueSystemParameterNotSettableError(
                f"System parameter {title} value cannot be set from the client"
            )
        result.append(spec)

    for field in result_schema.fields:
        if field.calc_mode != CalcMode.parameter or not is_registered_sys_name(field.title):
            continue
        value = resolve_sys_value(field.title, rci)
        if value is not None:
            result.append(ParameterValueSpec(field_id=field.guid, value=value))

    return result
