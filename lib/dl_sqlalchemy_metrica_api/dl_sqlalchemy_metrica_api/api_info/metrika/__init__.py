from __future__ import annotations

from enum import Enum

from dl_sqlalchemy_metrica_api.api_info.metrika.advertising import advertising_fields
from dl_sqlalchemy_metrica_api.api_info.metrika.hits import hits_fields
from dl_sqlalchemy_metrica_api.api_info.metrika.user_param import user_param_fields
from dl_sqlalchemy_metrica_api.api_info.metrika.visits import visits_fields


class MetrikaApiCounterSource(Enum):
    hits = "hits"
    visits = "visits"
    advertising = "advertising"
    user_param = "user_param"


metrica_fields_namespaces = MetrikaApiCounterSource


fields_by_namespace = {
    MetrikaApiCounterSource.hits: hits_fields,
    MetrikaApiCounterSource.visits: visits_fields,
    MetrikaApiCounterSource.advertising: advertising_fields,
    MetrikaApiCounterSource.user_param: user_param_fields,
}


fields_by_name = {f["name"]: f for f in sum(fields_by_namespace.values(), [])}


metrics_by_namespace = {
    cs: list(filter(lambda f: not f["is_dim"], fields)) for cs, fields in fields_by_namespace.items()
}


namespace_by_prefix = {
    "ym:pv": MetrikaApiCounterSource.hits,
    "ym:s": MetrikaApiCounterSource.visits,
    "ym:ad": MetrikaApiCounterSource.advertising,
    "ym:up": MetrikaApiCounterSource.user_param,
}


def get_namespace_by_name(name):
    name_parts = name.split(":", 2)
    if len(name_parts) != 3:
        raise ValueError('Unexpected name format: "%s".' % name)
    prefix = ":".join(name_parts[:2])
    if prefix not in namespace_by_prefix:
        raise ValueError('Unknown field prefix: "%s"' % prefix)
    return namespace_by_prefix[prefix]
