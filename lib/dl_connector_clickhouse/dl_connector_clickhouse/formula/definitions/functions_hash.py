import dl_formula.definitions.functions_hash as base

from dl_connector_clickhouse.formula.constants import ClickHouseDialect as D


DEFINITIONS_HASH = [
    base.MD5.for_dialect(D.CLICKHOUSE),
    base.SHA1.for_dialect(D.CLICKHOUSE),
    base.SHA256.for_dialect(D.CLICKHOUSE),
    base.MurmurHash2_64.for_dialect(D.CLICKHOUSE),
    base.SipHash64.for_dialect(D.CLICKHOUSE),
    base.IntHash64.for_dialect(D.CLICKHOUSE),
    base.CityHash64.for_dialect(D.CLICKHOUSE),
]
