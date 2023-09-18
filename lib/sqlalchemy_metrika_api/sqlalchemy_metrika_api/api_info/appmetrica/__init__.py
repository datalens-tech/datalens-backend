from __future__ import annotations

from enum import Enum

from sqlalchemy_metrika_api.api_info.appmetrica.audience import audience_fields
from sqlalchemy_metrika_api.api_info.appmetrica.audience_socdem import audience_socdem_fields
from sqlalchemy_metrika_api.api_info.appmetrica.client_events import client_events_fields
from sqlalchemy_metrika_api.api_info.appmetrica.crash_events import crash_events_fields
from sqlalchemy_metrika_api.api_info.appmetrica.installs import installs_fields
from sqlalchemy_metrika_api.api_info.appmetrica.push_events import push_events_fields


class AppMetricaFieldsNamespaces(Enum):
    installs = "installs"
    push_events = "push_events"
    client_events = "client_events"
    audience = "audience"
    audience_socdem = "audience_socdem"
    crash_events = "crash_events"


metrica_fields_namespaces = AppMetricaFieldsNamespaces


fields_by_namespace = {
    AppMetricaFieldsNamespaces.installs: installs_fields,
    AppMetricaFieldsNamespaces.push_events: push_events_fields,
    AppMetricaFieldsNamespaces.client_events: client_events_fields,
    AppMetricaFieldsNamespaces.audience: audience_fields,
    AppMetricaFieldsNamespaces.audience_socdem: audience_socdem_fields,
    AppMetricaFieldsNamespaces.crash_events: crash_events_fields,
}


fields_by_name = {f["name"]: f for f in sum(fields_by_namespace.values(), [])}


metrics_by_namespace = {
    cs: list(filter(lambda f: not f["is_dim"], fields)) for cs, fields in fields_by_namespace.items()
}


namespace_by_prefix = {
    "ym:ts": AppMetricaFieldsNamespaces.installs,
    "ym:pc": AppMetricaFieldsNamespaces.push_events,
    "ym:ce": AppMetricaFieldsNamespaces.client_events,
    "ym:u": AppMetricaFieldsNamespaces.audience,
    "ym:cr2": AppMetricaFieldsNamespaces.crash_events,
}


def get_namespace_by_name(name):
    name_parts = name.split(":", 2)
    if len(name_parts) != 3:
        raise ValueError('Unexpected name format: "%s".' % name)
    prefix = ":".join(name_parts[:2])
    if prefix not in namespace_by_prefix:
        raise ValueError('Unknown field prefix: "%s"' % prefix)
    return namespace_by_prefix[prefix]
